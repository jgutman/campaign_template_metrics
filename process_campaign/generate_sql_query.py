import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging, re, sys
from datetime import datetime
from argparse import ArgumentParser
from pathlib import Path
from .upload_redshift import extract_campaign_info


def offer_redemption(tm):
    if not tm.offer_campaign_name.isnull().all():

        return """LEFT JOIN dw.marketing_offers md
        ON md.internal_user_id = a.user_id
        AND md.offer_campaign_name = a.offer_campaign_name
        AND md.discount_name = a.discount_name
        AND md.offer_redeemed_at IS NOT NULL
        """

    elif not tm.discount_name.isnull().all():

        return """LEFT JOIN web.discounts d
        ON d.name = a.discount_name
        LEFT JOIN web.users_discounts ud
        ON d.id = ud.discount_id
        AND ud.user_id = a.user_id
        LEFT JOIN dw.marketing_discount_redemptions md
        ON md.internal_user_id = a.user_id
        AND md.discount_or_promo_id = ud.id
        """

    else:
        return ''


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

    promo_period_query = "\n    UNION ALL\n    ".join([
        """SELECT '{}' AS promo_period,
        '{}' AS end_date""".format(k, v)
        for k,v in promo_periods.iteritems()])

    discounts_query = offer_redemption(test_matrix)

    campaign_lists = """SELECT DISTINCT a.*
        , pp.*
        {offer_redeemed}
    FROM {tbl_name} a
    {discounts_query}
    CROSS JOIN (
    {promo_period_query}
    ) pp
    WHERE a.target_name IN ({targets})
    """.format(
        tbl_name = tbl_name,
        offer_redeemed =
            ', md.internal_user_id IS NOT NULL AS offer_redeemed' \
            if discounts_query != '' else '',
        discounts_query = discounts_query,
        promo_period_query = promo_period_query,
        targets = ",\n".join(["'{}'".format(target)
            for target in test_matrix.target_name]))

    compose_full_query = """WITH campaign_lists AS (
    {}),""".format(campaign_lists)

    inc_subscription_changes = info[['cancelations', 'cancelation_rate',
        'new_activations', 'activation_rate', 'reactivations',
        'reactivation_rate']].any()

    inc_active_at_end = info[['total_active_at_end', 'pct_active_at_end']].any()

    inc_boxes_ordered = info[['total_boxes_ordered', 'avg_boxes_ordered', 'gov',
        'aov', 'desserts_ordered', 'dessert_take_rate', 'ordered_nth_box',
        'ordered_ds']].any()

    inc_four_week_order = info[['total_ordered_week_4', 'pct_ordered_week_4',
        'avg_num_boxes_first_4_weeks']].any()

    inc_upgrade_events = info[['total_upgrades', 'total_downgrades']].any()

    inc_gift_card_orders = info['gift_cards_purchased']

    inc_used_the_app = info['total_using_the_app']

    inc_referrals = info['num_referrals_sent']

    boolean_metrics = []
    numeric_metrics = []
    joins = []

    if inc_subscription_changes:
        subscription_changes = """SELECT a.user_id
            , a.promo_period
            {change_q} = 'activation') AS activated
            {change_q} = 'reactivation') AS reactivated
            {change_q} = 'cancelation') AS canceled
        FROM campaign_lists a
        INNER JOIN dw.user_subscription_events use
            ON a.user_id = use.internal_user_id
            AND DATE(convert_timezone('America/New_York',
            subscription_changed_at))
            BETWEEN '{start_date}' AND a.end_date
        GROUP BY 1,2
        """.format(
            change_q = ', bool_or( subscription_status_change_event',
            start_date = start_date)

        boolean_metrics.extend(['activated', 'reactivated', 'canceled'])
        joins.append(('subscription_changes', 'sc'))

        compose_full_query += """\nsubscription_changes AS (
        {}),""".format(subscription_changes)

    if inc_active_at_end:
        last_change = """SELECT a.user_id
            , a.promo_period
            , max(use.created_at) last_change
        FROM campaign_lists a
        INNER JOIN web.user_membership_status_changes use
            ON a.user_id = use.user_id
            AND DATE(convert_timezone('America/New_York',
            use.created_at)) <= a.end_date
        GROUP BY 1,2
        """

        active_at_end = """SELECT a.user_id
            , a.promo_period
            , change_type IN (0,1,4) AS active_at_end
        FROM last_change a
        INNER JOIN web.user_membership_status_changes use
            ON a.user_id = use.user_id
            AND last_change = use.created_at
        """

        boolean_metrics.append('active_at_end')
        joins.append(('active_at_end', 'ae'))
        compose_full_query += """\nlast_change AS (
        {}),
        active_at_end AS (
        {}),""".format(last_change, active_at_end)

    if inc_boxes_ordered:
        if info.ordered_nth_box:
            boxes = [int(i) for i in info.ordered_nth_box.split(', ')]
            box_measure_cols = ['ordered_{}{}_box'.format(
                n, 'st' if n==1 else 'nd' if n==2 else 'rd' if n==3 else 'th'
            ) for n in boxes]
            box_measures = [', bool_or( nth_delivery = {}) AS {}'.format(
                    n, colname) for n, colname
                    in zip(boxes, box_measure_cols)]
            boolean_metrics.extend(box_measure_cols)
        else:
            box_measures = []

        if info.ordered_ds:
            ds_measure_col = 'ordered_ds_{}'.format(int(info.ordered_ds))
            ds_ordered = """, bool_or(mob.delivery_schedule_name = '{ds}')
                AS ordered_ds_{ds}""".format(ds = int(info.ordered_ds))
            boolean_metrics.append(ds_measure_col)

        else:
            ds_ordered = ""

        mob = """SELECT a.user_id
            , a.promo_period
            , delivery_schedule_name,
            min(delivery_date) AS delivery_date
        FROM campaign_lists a
        INNER JOIN dw.menu_order_boxes bo
            ON bo.internal_user_id = a.user_id
            AND delivery_date BETWEEN '{start_date}' AND a.end_date
            AND status <> 'canceled'
            AND delivery_schedule_type = 'normal'
        GROUP BY 1,2,3
        """.format(start_date = start_date)

        boxes_ordered = """SELECT mob.user_id
            , mob.promo_period
            , COUNT(DISTINCT mob.delivery_date) AS total_boxes_ordered
            {box_measures_joined}
            {ds_ordered}
            , SUM(gov) AS gov
            , SUM( CASE WHEN dessert_plates > 0 THEN 1 END )
                AS desserts_ordered
        FROM mob
        INNER JOIN dw.menu_order_boxes bo
            ON bo.internal_user_id = mob.user_id
            AND bo.delivery_date = mob.delivery_date
            AND bo.status <> 'canceled'
        GROUP BY 1,2
        """.format(
            box_measures_joined = '\n    '.join(box_measures),
            ds_ordered = ds_ordered,
            start_date = start_date)

        numeric_metrics.extend(['total_boxes_ordered', 'gov',
            'desserts_ordered'])
        joins.append(('boxes_ordered', 'bo'))
        compose_full_query += """\nmob AS (
        {}),
        boxes_ordered AS (
        {}),""".format(mob, boxes_ordered)

    if inc_four_week_order:
        cohorts = """select user_id
            , min(delivery_date) as first_delivery_date
        FROM mob
        GROUP BY 1
        """

        four_week_order = """SELECT mob.user_id
            , COUNT(DISTINCT mob.delivery_date) AS num_boxes_first_4_weeks
            , bool_or(datediff('week', cohorts.first_delivery_date,
                mob.delivery_date) = 3)
                AS ordered_week_4
        FROM cohorts
        INNER JOIN mob
            ON cohorts.user_id = mob.user_id
            AND datediff('week', cohorts.first_delivery_date,
            mob.delivery_date) < 4
        GROUP BY 1
        """

        boolean_metrics.append('ordered_week_4')
        numeric_metrics.append('num_boxes_first_4_weeks')
        joins.append(('four_week_order', 'fwo'))
        compose_full_query += """\ncohorts AS (
        {}),
        four_week_order AS (
        {}),""".format(cohorts, four_week_order)

    if inc_upgrade_events:
        upgrade_events_raw = """SELECT a.user_id
            , a.promo_period
            , CAST(json_extract_path_text(properties,
                'new_plan_dinners') AS INT) *
              CAST(json_extract_path_text(properties,
                'new_plan_servings') AS INT) AS new_plan_plates
            , CAST(json_extract_path_text(properties,
                'old_plan_dinners') AS INT) *
              CAST(json_extract_path_text(properties,
                'old_plan_servings') AS INT) AS old_plan_plates
        FROM campaign_lists a
        INNER JOIN dw.web_track_events wte
            ON wte.user_id = a.user_id
            AND wte.event = 'Subscription Plan Changed'
            AND DATE(convert_timezone('America/New_York',
            client_timestamp))
            BETWEEN '{start_date}' AND a.end_date
        """.format(start_date = start_date)

        upgrade_events = """SELECT a.user_id
            , a.promo_period
            , bool_or(new_plan_plates > old_plan_plates) AS upgraded
            , bool_or(new_plan_plates < old_plan_plates) AS downgraded
        FROM upgrade_events_raw a
        WHERE new_plan_plates <> old_plan_plates
        GROUP BY 1,2
        """

        boolean_metrics.extend(['upgraded', 'downgraded'])
        joins.append(('upgrade_events', 'ue'))
        compose_full_query += """\nupgrade_events_raw AS (
        {}),
        upgrade_events AS (
        {}),""".format(upgrade_events_raw, upgrade_events)

    if inc_gift_card_orders:
        gift_card_orders = """SELECT a.user_id
            , a.promo_period
            , TRUE as gift_card_purchase
        FROM campaign_lists a
        INNER JOIN dw.gift_card_orders gco
            ON gco.sender_internal_user_id = a.user_id
            AND DATE(convert_timezone('America/New_York',
                gift_card_order_placed_at))
            BETWEEN '{start_date}' AND a.end_date
        """.format(start_date = start_date)

        boolean_metrics.append('gift_card_purchase')
        joins.append(('gift_card_orders', 'gc'))
        compose_full_query += """\ngift_card_orders AS (
        {}),""".format(gift_card_orders)

    if inc_used_the_app:
        used_the_app = """SELECT a.user_id
            , a.promo_period
            , TRUE as used_the_app
        FROM campaign_lists a
        INNER JOIN dw.users u
            ON a.user_id = u.internal_user_id
        INNER JOIN (
            SELECT external_user_id
            , client_timestamp
            FROM dw.app_track_events
            WHERE DATE(client_timestamp) >= '{start_date}'
            UNION all
            SELECT external_user_id
            , client_timestamp
            FROM dw.android_events
            WHERE DATE(client_timestamp) >= '{start_date}'
        ) app_visits
            ON app_visits.external_user_id = u.external_id
            AND client_timestamp <= a.end_date
        GROUP BY 1,2
        """.format(start_date = start_date)

        boolean_metrics.append('used_the_app')
        joins.append(('used_the_app', 'app'))
        compose_full_query += """\nused_the_app AS (
        {}),""".format(used_the_app)

    if inc_referrals:
        referrals_q = """SELECT a.user_id
            , a.promo_period
            , COUNT(DISTINCT sent_to_email) AS num_referrals_sent
            , TRUE AS sent_referral
        FROM campaign_lists a
        INNER JOIN dw.user_referral_invites r
            ON r.referrer_internal_user_id = a.user_id
            AND DATE(convert_timezone('America/New_York',
                sent_at))
            BETWEEN '{start_date}' AND a.end_date
        GROUP BY 1,2
        """.format(start_date = start_date)

        numeric_metrics.append('num_referrals_sent')
        boolean_metrics.append('sent_referral')
        joins.append(('referrals', 'ref'))
        compose_full_query += """\nreferrals AS (
        {}),""".format(referrals_q)

    boolean_metrics_q = [', COALESCE({a}, false) AS {a}'.format(a = x)
        for x in boolean_metrics]
    numeric_metrics_q = [', COALESCE({a}, 0) AS {a}'.format(a = x)
        for x in numeric_metrics]

    join_q = ["""LEFT JOIN {tbl_name} {a}
        ON {a}.user_id = a.user_id
        AND {a}.promo_period = a.promo_period""".format(
            tbl_name = tbl_name,
            a = alias)
        if tbl_name != 'four_week_order'
        else """LEFT JOIN {tbl_name} {a}
        ON {a}.user_id = a.user_id""".format(
            tbl_name = tbl_name,
            a = alias)
        for tbl_name, alias in joins]

    individual_metrics = """SELECT a.*
        {boolean_metrics}
        {numeric_metrics}
    FROM campaign_lists a
        {join_query}
        """.format(
        boolean_metrics = '\n    '.join(boolean_metrics_q),
        numeric_metrics = '\n    '.join(numeric_metrics_q),
        join_query = '\n    '.join(join_q))

    if info.redeemed_offer_discount:
        boolean_metrics.append('offer_redeemed')

    aggregate_boolean = [', SUM( CASE WHEN a.{a} THEN 1 ELSE 0 END) AS {a}'.format(
        a = a) for a in boolean_metrics]
    aggregate_numeric = [', SUM(a.{a}) AS {a}'.format(
        a = a) for a in numeric_metrics]

    aggregates = """SELECT a.promo_period
        , '{start_date}' AS start_date
        , a.end_date
        , r.{responder_action} AS responder
        , {test_matrix_cols}
        , COUNT(DISTINCT a.user_id) AS total_segment_size
        {agg_bools}
        {agg_nums}
    FROM individual_metrics a
    INNER JOIN individual_metrics r
        ON a.user_id = r.user_id
        AND r.promo_period = 'promo_period'
    GROUP BY {join_nums}
    """.format(
        start_date = start_date,
        responder_action = info.responder_action,
        test_matrix_cols = '\n    , '.join('a.{}'.format(col) for col in test_matrix.columns),
        agg_bools = '\n    '.join(aggregate_boolean),
        agg_nums = '\n    '.join(aggregate_numeric),
        join_nums = ','.join([str(i+1)
            for i in range(test_matrix.shape[1]+4)]))

    compose_full_query += """
        individual_metrics AS (
        {})
        {}""".format(
            individual_metrics,
            aggregates)
    return compose_full_query


def compute_and_output_metrics(data, info, path, tm_cols):
    data = data.rename(columns = {
        'canceled': 'cancelations',
        'activated': 'new_activations',
        'reactivated': 'reactivations',
        'active_at_end': 'total_active_at_end',
        'upgraded': 'total_upgrades',
        'downgraded': 'total_downgrades',
        'gift_card_purchase': 'gift_cards_purchased',
        'ordered_week_4': 'total_ordered_week_4',
        'offer_redeemed': 'redeemed_offer_discount',
        'used_the_app': 'total_using_the_app',
        'total_segment_size': 'segment_responder_size'})

    id_cols = ['target_name', 'responder']

    totals = data[id_cols + ['segment_responder_size']].groupby('target_name')
    response_rates = {target: data.loc[data.responder == True,
        'segment_responder_size'] / data.segment_responder_size.sum()
        for target, data in totals}
    response_rates = pd.Series({target: v.iloc[0]
        if len(v) else 0 for target, v in response_rates.items()},
        name = 'response_rate')
    data = data.join(response_rates, on = 'target_name')
    data['response_rate'] = ["{:.2%}".format(k)
        for k in data.response_rate]

    unpivot_cols = [x for x in tm_cols if x != 'target_name']
    unpivot_cols.extend(['segment_responder_size', 'response_rate'])
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
        boxes_ordered = [x.group() for x in boxes_ordered if x]
        metrics.remove('ordered_nth_box')
        metrics.extend(boxes_ordered)

    if 'reactivation_rate' in metrics:
        data['reactivation_rate'] = ["{:.2%}".format(k)
            if np.isfinite(k) else ''
            for k in data.reactivations / data.segment_responder_size]

    if 'activation_rate' in metrics:
        data['activation_rate'] = ["{:.2%}".format(k)
            if np.isfinite(k) else ''
            for k in data.new_activations / data.segment_responder_size]

    if 'cancelation_rate' in metrics:
        data['cancelation_rate'] = ["{:.2%}".format(k)
            if np.isfinite(k) else ''
            for k in data.cancelations / data.segment_responder_size]

    if 'avg_boxes_ordered' in metrics:
        data['avg_boxes_ordered'] = ["{}".format(round(k,2))
            if np.isfinite(k) else ''
            for k in data.total_boxes_ordered / data.segment_responder_size]

    if 'aov' in metrics:
        data['aov'] = ["${:.2f}".format(k)
            if np.isfinite(k) else ''
            for k in data.gov / data.total_boxes_ordered]

    if 'gov' in metrics:
        data['gov'] = ["${:,.2f}".format(k)
            if np.isfinite(k) else ''
            for k in data.gov]

    if 'dessert_take_rate' in metrics:
        data['dessert_take_rate'] = ["{:.2%}".format(k)
            if np.isfinite(k) else ''
            for k in data.desserts_ordered / data.total_boxes_ordered]

    if 'pct_redeemed' in metrics:
        data['pct_redeemed'] = ["{:.2%}".format(k)
            if np.isfinite(k) else ''
            for k in data.redeemed_offer_discount / data.segment_responder_size]

    if 'pct_active_at_end' in metrics:
        data['pct_active_at_end'] = ["{:.2%}".format(k)
            if np.isfinite(k) else ''
            for k in data.total_active_at_end / data.segment_responder_size]

    if 'ordered_ds' in metrics:
        ds_ordered = [re.match(r'ordered_ds_\d{4,}', col)
            for col in data.columns]
        ds_ordered = [x.group() for x in ds_ordered if x]
        metrics.remove('ordered_ds')
        metrics.extend(ds_ordered)

    if 'pct_ordered_week_4' in metrics:
        data['pct_ordered_week_4'] = ["{:.2%}".format(k)
            if np.isfinite(k) else ''
            for k in data.total_ordered_week_4 / data.segment_responder_size]

    if 'avg_num_boxes_first_4_weeks' in metrics:
        data['avg_num_boxes_first_4_weeks'] = ["{}".format(round(k,2))
            if np.isfinite(k) else ''
            for k in data.num_boxes_first_4_weeks / data.segment_responder_size]

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
    data_wide['response_rate']
    data_wide = data_wide.rename_axis(
        ['target name', 'responder ({})'.format(info.responder_action)])
    data_wide = data_wide.rename(columns = lambda x: x.replace('_', ' '))
    data_wide.sort_index().to_csv(path, index = True)


def main(args):
    logging.basicConfig(
        level=logging.INFO,
        format = '{asctime} {name:12s} {levelname:8s} {message}',
        datefmt = '%m-%d %H:%M:%S',
        style = '{',
        stream=sys.stdout)

    err = logging.StreamHandler(sys.stderr)
    err.setLevel(logging.ERROR)
    out = logging.StreamHandler(sys.stdout)
    out.setLevel(logging.INFO)
    logging.getLogger(__name__).addHandler(out)
    logging.getLogger(__name__).addHandler(err)

    [logging.info('Input argument {} set to {}'.format(k, v))
        for k,v in vars(args).items()]

    campaign_dir, test_matrix, campaign_info = extract_campaign_info(args)
    query = build_query(campaign_info, test_matrix)
    with open(str(Path(campaign_dir, 'generated_query.sql')), 'w') as f:
        f.write(query)
    logging.info('Query generated and written to disk at {}'.format(
        str(Path(campaign_dir, 'generated_query.sql'))))

    engine = create_engine("{driver}://{host}:{port}/{dbname}".format(
          driver = "postgresql+psycopg2",
          host = "localhost",
          port = 5439,
          dbname = "production"))

    tbl_name = campaign_info.campaign_short_name.strip().lower()
    if not engine.has_table(tbl_name, schema = 'analytics'):
        logging.error('Table `analytics.{}` not found'.format(tbl_name))
    else:
        logging.info('Table `analytics.{}` found successfully'.format(tbl_name))

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
