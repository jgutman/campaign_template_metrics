import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, MetaData, Table
import logging, re, sys
from datetime import datetime
from argparse import ArgumentParser
from pathlib import Path
from upload_redshift import extract_campaign_info


def offer_redemption(tm):
    if not tm.offer_campaign_name.isnull().all():
        discounts_table = 'marketing_offers'
        discounts_query_list = [
        """(md.offer_campaign_name = '{offer_campaign_name}'
        AND md.discount_name = '{discount_name}'
        AND a.target_name = '{target_name}')
        """.format(
            offer_campaign_name = i.offer_campaign_name,
            discount_name = i.discount_name,
            target_name = i.target_name
        ) for i in tm[tm.offer_campaign_name.notnull()
            ].itertuples(index = False)]

    elif not tm.discount_name.isnull().all():
        discounts_table = 'marketing_discounts'
        discounts_query_list = [
        """(md.discount_name = '{discount_name}'
        AND a.target_name = '{target_name}')
        """.format(
            discount_name = i.discount_name,
            target_name = i.target_name
        ) for i in tm[tm.discount_name.notnull()
            ].itertuples(index = False)]
    else:
        return ''

    discounts_query = """LEFT JOIN dw.{discounts_table} md
    ON md.internal_user_id = a.user_id
    AND ({dq_joins})""".format(
        discounts_table = discounts_table,
        dq_joins = 'OR '.join(discounts_query_list))

    return discounts_query


def build_query(info, test_matrix):
    tbl_name = 'analytics.{}'.format(
        info.campaign_short_name.strip().lower())

    promo_periods = (info[[col for col in info.dropna().index
        if col.endswith('_end_date')]]
        .apply(lambda x: (datetime.now() if x == 'current_date' else x)
        .strftime('%Y-%m-%d')))
    promo_periods.index = [x.replace('_end_date', '')
        for x in promo_periods.index]

    start_date = info.start_date.strftime('%Y-%m-%d')

    promo_period_query = "\n\tUNION ALL\n\t".join([
        """SELECT '{}' AS promo_period,
        '{}' AS end_date""".format(k, v)
        for k,v in promo_periods.iteritems()])

    discounts_query = offer_redemption(test_matrix)

    campaign_lists = """SELECT a.*
        , pp.*
        {offer_redeemed}
    FROM {tbl_name} a
    {discounts_query}
    CROSS JOIN (
    {promo_period_query}
    ) pp
    """.format(
        tbl_name = tbl_name,
        offer_redeemed =
            ', md.internal_user_id IS NOT NULL AS offer_redeemed' \
            if discounts_query != '' else '',
        discounts_query = discounts_query,
        promo_period_query = promo_period_query)

    subscription_changes = """SELECT a.user_id
        , a.promo_period
        {change_q} = 'activation') AS activated
        {change_q} = 'reactivation') AS reactivated
        {change_q} = 'cancelation') AS canceled
    FROM campaign_lists a
    INNER JOIN dw.user_subscription_events use
        ON a.user_id = use.internal_user_id
        AND {date_q} >= '{start_date}'
        AND {date_q} <= a.end_date
    GROUP BY 1,2
    """.format(
        change_q = ', bool_or( subscription_status_change_event',
        date_q = """DATE(convert_timezone('America/New_York',
        subscription_changed_at))""",
        start_date = start_date)

    last_change = """SELECT a.user_id
        , a.promo_period
        , max(subscription_changed_at) last_change
    FROM campaign_lists a
    INNER JOIN dw.user_subscription_events use
        ON a.user_id = use.internal_user_id
        AND {date_q} <= a.end_date
    GROUP BY 1,2
    """.format(
        date_q = """DATE(convert_timezone('America/New_York',
        subscription_changed_at))""")

    active_at_end = """SELECT a.user_id
        , a.promo_period
        , subscription_status_change_event LIKE '%activation'
            AS active_at_end
    FROM last_change a
    INNER JOIN dw.user_subscription_events use
        ON a.user_id = use.internal_user_id
        AND last_change = subscription_changed_at
        """

    if info.ordered_nth_box:
        boxes = [int(i) for i in info.ordered_nth_box.split(', ')]
        box_measure_cols = ['ordered_{}{}_box'.format(
            n, 'st' if n==1 else 'nd' if n==2 else 'rd' if n==3 else 'th'
        ) for n in boxes]
        box_measures = [', bool_or( nth_delivery = {}) AS {}'.format(
                n, colname) for n, colname
                in zip(boxes, box_measure_cols)]
    else:
        box_measures = box_measure_cols = []

    boxes_ordered = """SELECT a.user_id
        , a.promo_period
        , COUNT( DISTINCT delivery_schedule_name ) AS total_boxes_ordered
        {box_measures_joined}
        , SUM(gov) AS gov
        , SUM( CASE WHEN dessert_plates > 0 THEN 1 END )
            AS desserts_ordered
    FROM campaign_lists a
    INNER JOIN dw.menu_order_boxes bo
        ON bo.internal_user_id = a.user_id
        AND ship_date >= '{start_date}'
        AND ship_date <= a.end_date
        AND status <> 'canceled'
    GROUP BY 1,2
    """.format(
        box_measures_joined = '\n\t'.join(box_measures),
        start_date = start_date)

    boolean_metrics = ['active_at_end', 'canceled', 'activated',
        'reactivated'] + box_measure_cols
    numeric_metrics = ['total_boxes_ordered', 'gov', 'desserts_ordered']

    boolean_metrics_q = [', COALESCE({a}, false) AS {a}'.format(a = x)
        for x in boolean_metrics]
    numeric_metrics_q = [', COALESCE({a}, 0) AS {a}'.format(a = x)
        for x in numeric_metrics]

    joins = [('subscription_changes', 'sc'),
        ('active_at_end', 'ae'),
        ('boxes_ordered', 'bo')]
    join_q = ["""LEFT JOIN {tbl_name} {a}
        ON {a}.user_id = a.user_id
        AND {a}.promo_period = a.promo_period""".format(
            tbl_name = tbl_name,
            a = alias) for tbl_name, alias in joins]

    individual_metrics = """SELECT a.*
        {boolean_metrics}
        {numeric_metrics}
    FROM campaign_lists a
        {join_query}
        """.format(
        boolean_metrics = '\n\t'.join(boolean_metrics_q),
        numeric_metrics = '\n\t'.join(numeric_metrics_q),
        join_query = '\n\t'.join(join_q))

    responder_metric = """SELECT user_id
        , {responder_action} AS responder
        FROM individual_metrics
        WHERE promo_period = 'promo_period'
        """.format(responder_action = info.responder_action)

    if info.redeemed_offer_discount:
        boolean_metrics.append('offer_redeemed')

    aggregate_boolean = [', SUM( CASE WHEN {a} THEN 1 ELSE 0 END) AS {a}'.format(
        a = a) for a in boolean_metrics]
    aggregate_numeric = [', SUM({a}) AS {a}'.format(
        a = a) for a in numeric_metrics]

    aggregates = """SELECT a.promo_period
        , '{start_date}' AS start_date
        , a.end_date
        , COALESCE(r.responder, false) AS responder
        , {test_matrix_cols}
        , COUNT(*) AS total_segment_size
        {agg_bools}
        {agg_nums}
    FROM individual_metrics a
    LEFT JOIN responder_metric r
        ON a.user_id = r.user_id
    GROUP BY {join_nums}
    """.format(
        start_date = start_date,
        test_matrix_cols = '\n\t , '.join(test_matrix.columns),
        agg_bools = '\n\t'.join(aggregate_boolean),
        agg_nums = '\n\t'.join(aggregate_numeric),
        join_nums = ','.join([str(i+1)
            for i in range(test_matrix.shape[1]+4)]))

    compose_full_query = """WITH campaign_lists AS (
        {}),
        subscription_changes AS (
        {}),
        last_change AS (
        {}),
        active_at_end AS (
        {}),
        boxes_ordered AS (
        {}),
        individual_metrics AS (
        {}),
        responder_metric AS (
        {})
        {}""".format(
            campaign_lists,
            subscription_changes,
            last_change,
            active_at_end,
            boxes_ordered,
            individual_metrics,
            responder_metric,
            aggregates)

    return compose_full_query


def update_campaign_table(engine, tbl_name, columns):

    if 'internal_user_id' in columns:
        rename_query = """ALTER TABLE analytics.{}
        RENAME COLUMN internal_user_id to user_id
        """.format(tbl_name)
        with engine.begin() as connection:
            connection.execute(rename_query)
        logging.info('Replaced internal_user_id column with user_id')

    elif 'prospect_id' in columns:
        join_query = """SELECT a.*,
            u.internal_user_id AS user_id
        FROM analytics.{} a
        LEFT JOIN dw.users u
        ON u.internal_marketing_prospect_id = a.prospect_id
        """.format(tbl_name)

        data = pd.read_sql_query(text(join_query), engine)
        data.to_sql(tbl_name, con = engine,
            schema = 'analytics', if_exists = 'replace', index = False)
        logging.info('Joined on internal_marketing_prospect_id and added user_id')

    elif 'external_id' in columns:
        join_query = """SELECT a.*,
            u.internal_user_id AS user_id
        FROM analytics.{} a
        LEFT JOIN dw.users u
        ON u.external_id = a.external_id
        """.format(tbl_name)

        data = pd.read_sql_query(text(join_query), engine)
        data.to_sql(tbl_name, con = engine,
            schema = 'analytics', if_exists = 'replace', index = False)
        logging.info('Joined on external_id and added user_id')

    elif 'email' in columns:
        join_query = """SELECT a.*,
            u.internal_user_id AS user_id
        FROM analytics.{} a
        LEFT JOIN dw.users u
        ON u.email = a.email
        """.format(tbl_name)

        data = pd.read_sql_query(text(join_query), engine)
        data.to_sql(tbl_name, con = engine,
            schema = 'analytics', if_exists = 'replace', index = False)
        logging.info('Joined on registered user email and added user_id')

    else:
        logging.error('Could not find any id column in {}: {}'.format(
            table_name, '\n'.join(
                ['user_id', 'internal_user_id', 'prospect_id',
                'external_id', 'email'])))


def compute_and_output_metrics(data, info, path, tm_cols):
    data = data.rename(columns = {'canceled': 'cancelations',
        'activated': 'new_activations', 'reactivated': 'reactivations',
        'active_at_end': 'total_active_at_end',
        'offer_redeemed': 'redeemed_offer_discount',
        'total_segment_size': 'segment_responder_size'})

    id_cols = ['target_name', 'responder']
    unpivot_cols = [x for x in tm_cols if x != 'target_name']
    unpivot_cols.append('segment_responder_size')
    tm_data = data[id_cols + unpivot_cols].drop_duplicates()
    tm_data = tm_data.set_index(id_cols)

    data['date_range'] = ['{} - {}'.format(i.start_date.strftime('%m/%d'),
        i.end_date.strftime('%m/%d'))
        for i in data.itertuples(index=False)]

    metrics = list(info.loc['cancelations':][info.astype(bool)].index)
    metrics.insert(0, 'date_range')

    if 'ordered_nth_box' in metrics:
        boxes_ordered = [re.match(r'ordered_\d{1,}[a-z]{2}_box', col)
            for col in data.columns]
        boxes_ordered = [x.group(0) for x in boxes_ordered if x]
        metrics.remove('ordered_nth_box')
        metrics.extend(boxes_ordered)

    if 'reactivation_rate' in metrics:
        data['reactivation_rate'] = ["{:.2%}".format(k)
            if np.isfinite(k) else ''
            for k in data.reactivations/data.segment_responder_size]

    if 'activation_rate' in metrics:
        data['activation_rate'] = ["{:.2%}".format(k)
            if np.isfinite(k) else ''
            for k in data.new_activations/data.segment_responder_size]

    if 'cancelation_rate' in metrics:
        data['cancelation_rate'] = ["{:.2%}".format(k)
            if np.isfinite(k) else ''
            for k in data.cancelations/data.segment_responder_size]

    if 'avg_boxes_ordered' in metrics:
        data['avg_boxes_ordered'] = ["{:.2}".format(k)
            if np.isfinite(k) else ''
            for k in data.total_boxes_ordered/data.segment_responder_size]

    if 'aov' in metrics:
        data['aov'] = ["${:.2f}".format(k)
            if np.isfinite(k) else ''
            for k in data.gov/data.total_boxes_ordered]

    if 'gov' in metrics:
        data['gov'] = ["${:,.2f}".format(k)
            if np.isfinite(k) else ''
            for k in data.gov]

    if 'dessert_take_rate' in metrics:
        data['dessert_take_rate'] = ["{:.2%}".format(k)
            if np.isfinite(k) else ''
            for k in data.desserts_ordered/data.total_boxes_ordered]

    if 'pct_redeemed' in metrics:
        data['pct_redeemed'] = ["{:.2%}".format(k)
            if np.isfinite(k) else ''
            for k in data.redeemed_offer_discount/data.segment_responder_size]

    if 'pct_active_at_end' in metrics:
        data['pct_active_at_end'] = ["{:.2%}".format(k)
            if np.isfinite(k) else ''
            for k in data.total_active_at_end/data.segment_responder_size]

    data = data[id_cols + ['promo_period'] + metrics]
    pivot = data.pivot_table(index = id_cols,
        columns = 'promo_period', aggfunc = lambda x: x)

    promo_periods = [i.replace('_end_date', '')
        for i in info.index
        if i.endswith('_end_date') and info.notnull().loc[i]]

    data_wide = [pivot.xs(i, axis = 1, level = 'promo_period')[metrics].rename(
        columns = lambda x: '{}\n{}'.format(x, i))
        for i in promo_periods]

    data_wide = tm_data.join(data_wide)
    responders = ['responder' if i else 'non-responder'
        for i in data_wide.index.levels[1]]
    data_wide.index = data_wide.index.set_levels(
        responders, level = 'responder')
    data_wide = data_wide.rename_axis(
        ['target name', 'responder ({})'.format(info.responder_action)])
    data_wide = data_wide.rename(columns = lambda x: x.replace('_', ' '))
    data_wide.to_csv(path, index = True)


def main(args):
    logging.basicConfig(
        level=logging.INFO,
        format = '{asctime} {name:12s} {levelname:8s} {message}',
        datefmt = '%m-%d %H:%M:%S',
        style = '{' )

    [logging.info('Input argument {} set to {}'.format(k, v))
        for k,v in vars(args).items()]

    campaign_dir, test_matrix, campaign_info = extract_campaign_info(args)
    query = build_query(campaign_info, test_matrix)
    with open(str(Path(campaign_dir, 'generated_query.sql')), 'w') as f:
        f.write(query)

    engine = create_engine("{driver}://{host}:{port}/{dbname}".format(
          driver = "postgresql+psycopg2",
          host = "localhost",
          port = 5439,
          dbname = "production"))

    tbl_name = campaign_info.campaign_short_name.strip().lower()
    if not engine.has_table(tbl_name, schema = 'analytics'):
        logging.error('table `analytics.{}` not found'.format(tbl_name))

    campaign_lists = Table(tbl_name,
        MetaData(), schema = 'analytics', autoload_with = engine)
    if 'user_id' not in campaign_lists.columns:
        update_campaign_table(engine, tbl_name, campaign_lists.columns)

    data = pd.read_sql_query(text(query), engine,
        parse_dates = ['start_date', 'end_date'])
    logging.info('Aggregate data pulled successfully from analytics.{}'.format(
        tbl_name))

    output_file = '{}_report_metrics.csv'.format(
        campaign_info.campaign_name.strip())
    compute_and_output_metrics(data, campaign_info,
        tm_cols = test_matrix.columns,
        path = Path(campaign_dir, output_file))


if __name__ == '__main__':
    parser = ArgumentParser('Compute and save campaign metrics.')
    parser.add_argument('--root_dir',
        help = 'path to directory containing information on all campaigns',
        default = str(Path(Path.home(),
            'Google Drive File Stream', 'My Drive',
            'Reformatted Prioritized Campaign Lists')))
    parser.add_argument('campaign_dir',
        help = 'name of directory for desired campaign')
    args = parser.parse_args()
    main(args)
