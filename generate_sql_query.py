import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table
import yaml, logging, re, os, sys
import yamlordereddictloader
from datetime import datetime


def parse_if_list(values):
    if type(values) == list:
        return ', '.join("'{}'".format(v) for v in values)
    else:
        return "'{}'".format(values)


def get_cum_sub_changes(periods):
    periods = list(periods)

    earlier_periods_string = [', '.join(["'{}'".format(v)
        for v in periods[:i+1]])
        for i,j in enumerate(periods)]

    union_list = ["""\t\tSELECT lsc.user_id,
        text('{name}') AS promo_period,
        max(last_change) AS last_change
        FROM last_subscription_change lsc
        WHERE promo_period IN ({earlier_periods})
        GROUP BY 1
    """.format(name = i, earlier_periods = j)
        for i, j in zip(periods, earlier_periods_string)]

    query = '\tUNION ALL\n'.join(union_list)
    return query


def build_query(options, table_name, groups):
    # build subquery for marketing offers/discounts redemptions
    groups = list(groups) + ['responder']

    if 'offer_campaign_name' in options['campaign_discount_names'][0]:
        discounts_table = 'marketing_offers'
        discounts_query_list = [
        """(md.offer_campaign_name = '{offer_campaign_name}'
        AND md.discount_name = '{discount_name}'
        AND {column_conditions})
        """.format(
            offer_campaign_name = i['offer_campaign_name'],
            discount_name = i['discount_name'],
            column_conditions = '\n\tAND '.join([
                "a.{col} IN ({value})".format(
                col = k, value = parse_if_list(v))
                for k,v in i['columns'].items()])
        ) for i in options['campaign_discount_names']]

    else:
        discounts_table = 'marketing_discounts'
        discounts_query_list = [
        """(md.discount_name = '{discount_name}'
        AND {column_conditions})
        """.format(
            discount_name = i['discount_name'],
            column_conditions = '\n\tAND '.join([
                "a.{col} IN ({value})".format(
                col = k, value = parse_if_list(v))
                for k,v in i['columns'].items()])
        ) for i in options['campaign_discount_names']]

    discounts_query = "OR ".join(discounts_query_list)

    promo_period_query = "\n\tUNION ALL\n\t".join([
        "SELECT '{}' AS promo_period".format(k)
        for k in options['date_ranges']['end_dates'].keys()])

    # build promo_period column for subscription changes
    subscription_dates_query_list = [
    """WHEN {date_q} <= '{promo_period_date}'
    THEN '{promo_period}'
    """.format(
            date_q = """DATE(convert_timezone('America/New_York',
            subscription_changed_at))""",
            promo_period_date = datetime.now().strftime('%Y-%m-%d') \
                if end_date == 'current_date' else end_date,
            promo_period = promo_period)
        for promo_period, end_date in
            options['date_ranges']['end_dates'].items()]
    subscription_dates_query = 'CASE {} END AS promo_period'.format(
        ''.join(subscription_dates_query_list))

    # build promo_period column for menu boxes ordered
    boxes_dates_query_list = [
    """WHEN {date_q} <= '{promo_period_date}'
    THEN '{promo_period}'
    """.format(
            date_q = 'ship_date',
            promo_period_date = datetime.now().strftime('%Y-%m-%d') \
                if end_date == 'current_date' else end_date,
            promo_period = promo_period)
        for promo_period, end_date in
            options['date_ranges']['end_dates'].items()]
    boxes_dates_query = 'CASE {} END AS promo_period'.format(
        ''.join(boxes_dates_query_list))

    segment_query = ",\n\t\t".join(['a.{}'.format(x) for x in groups])

    boxes_ordered_sums = ['total_boxes_ordered', 'gov',
        '_1st_boxes_ordered', '_4th_boxes_ordered', '_6th_boxes_ordered',
        'desserts_ordered']
    coalesce_boxes_subquery = ",\n\t".join(
        ["COALESCE(bo.{x}, 0) AS {x}".format(x = x) for x in boxes_ordered_sums])
    sum_boxes_subquery = ",\n\t".join(
        ["SUM({x}) AS {x}".format(x = x) for x in boxes_ordered_sums])

    cum_subscription_changes_query = get_cum_sub_changes(
        options['date_ranges']['end_dates'].keys())

    query = """
    WITH campaign_lists AS (
        SELECT a.*,
          md.internal_user_id IS NOT NULL AS responder,
          pp.promo_period
        FROM {table_name} a
        LEFT JOIN dw.{discounts_table} md
          ON md.internal_user_id = a.user_id
          AND ({discounts_query})
        CROSS JOIN (
        {promo_period_query}
        ) pp
    ),
      last_subscription_change AS (
        SELECT a.user_id,
          {subscription_dates_query},
          max(subscription_changed_at) AS last_change
        FROM campaign_lists a
        INNER JOIN dw.user_subscription_events use
          ON a.user_id = use.internal_user_id
          AND DATE(convert_timezone('America/New_York',
            subscription_changed_at)) >= '{start_date}'
        GROUP BY 1,2
    ),
      cum_subscription_changes AS (
      {cum_subscription_changes_query}
    ),
      subscription_changes AS (
        SELECT use.internal_user_id,
          csc.promo_period,
          use.subscription_status_change_event
        FROM cum_subscription_changes csc
        INNER JOIN dw.user_subscription_events use
          ON csc.user_id = use.internal_user_id
          AND csc.last_change = use.subscription_changed_at
    ),
      boxes_ordered AS (
        SELECT bo.internal_user_id,
          {boxes_dates_query},
          COUNT(*) AS total_boxes_ordered,
          CASE WHEN bool_or(nth_delivery = 1) THEN 1 END AS _1st_boxes_ordered,
          CASE WHEN bool_or(nth_delivery = 4) THEN 1 END AS _4th_boxes_ordered,
          CASE WHEN bool_or(nth_delivery = 6) THEN 1 END AS _6th_boxes_ordered,
          SUM(gov) AS gov,
          SUM(CASE WHEN dessert_plates > 0 THEN 1 END) AS desserts_ordered
      FROM campaign_lists a
      INNER JOIN dw.menu_order_boxes bo
      ON bo.internal_user_id = a.user_id
        AND ship_date >= '{start_date}'
        AND status <> 'canceled'
      GROUP BY 1,2
    ),
      individual_metrics AS (
        SELECT a.*,
          COALESCE(sc.subscription_status_change_event = 'cancelation',
            false) AS canceled,
          COALESCE(sc.subscription_status_change_event = 'activation',
            false) AS activated,
          COALESCE(sc.subscription_status_change_event = 'reactivation',
            false) AS reactivated,
          {coalesce_boxes_subquery}
      FROM campaign_lists a
      LEFT JOIN subscription_changes sc
      ON sc.internal_user_id = a.user_id
        AND sc.promo_period = a.promo_period
      LEFT JOIN boxes_ordered bo
      ON bo.internal_user_id = a.user_id
        AND bo.promo_period = a.promo_period
    )
      SELECT {segment_query},
        promo_period,
        COUNT(*) AS total,
        SUM(CASE WHEN canceled THEN 1 END) AS cancelations,
        SUM(CASE WHEN activated THEN 1 END) AS new_activations,
        SUM(CASE WHEN reactivated THEN 1 END) AS reactivations,
        {sum_boxes_subquery}
      FROM individual_metrics a
      GROUP BY {groupings}
    """.format(
        table_name = table_name,
        discounts_table = discounts_table,
        discounts_query = discounts_query,
        promo_period_query = promo_period_query,
        subscription_dates_query = subscription_dates_query,
        boxes_dates_query = boxes_dates_query,
        cum_subscription_changes_query = cum_subscription_changes_query,
        start_date = options['date_ranges']['start_date'],
        segment_query = segment_query,
        coalesce_boxes_subquery = coalesce_boxes_subquery,
        sum_boxes_subquery = sum_boxes_subquery,
        groupings = ', '.join([str(i+1) for i in range(len(groups) + 1)])
    )
    return query


def update_campaign_table(engine, table_name, table):
    schema_prefix = 'analytics.'

    if 'internal_user_id' in table.columns:
        rename_query = """
        ALTER TABLE {}
        RENAME COLUMN internal_user_id to user_id
        """.format(table_name)
        with engine.begin() as connection:
            connection.execute(rename_query)
        logging.info('Replace internal_user_id column with user_id')

    elif 'prospect_id' in table.columns:
        join_query = """
        SELECT a.*,
            u.internal_user_id AS user_id
        FROM {} a
        LEFT JOIN dw.users u
        ON u.internal_marketing_prospect_id = a.prospect_id
        """.format(table_name)

        data = pd.read_sql_query(text(join_query), engine)
        data.to_sql(table_name.replace(schema_prefix, ''), con = engine,
            schema = 'analytics', if_exists = 'replace', index = False)
        logging.info('Joined on internal_marketing_prospect_id and added user_id')

    elif 'external_id' in table.columns:
        join_query = """
        SELECT a.*,
            u.internal_user_id AS user_id
        FROM {} a
        LEFT JOIN dw.users u
        ON u.external_id = a.external_id
        """.format(table_name)

        data = pd.read_sql_query(text(join_query), engine)
        data.to_sql(table_name.replace(schema_prefix, ''), con = engine,
            schema = 'analytics', if_exists = 'replace', index = False)
        logging.info('Joined on external_id and added user_id')

    elif 'email' in table.columns:
        join_query = """
        SELECT a.*,
            u.internal_user_id AS user_id
        FROM {} a
        LEFT JOIN dw.users u
        ON u.email = a.email
        """.format(table_name)

        data = pd.read_sql_query(text(join_query), engine)
        data.to_sql(table_name.replace(schema_prefix, ''), con = engine,
            schema = 'analytics', if_exists = 'replace', index = False)
        logging.info('Joined on registered user email and added user_id')

    else:
        logging.info('Could not find any id column in {}: {}'.format(
            table_name,
            '\n'.join(['user_id', 'internal_user_id', 'prospect_id',
                'external_id', 'email'])))


def compute_and_output_metrics(data, metrics, path, groups):
    groups = list(groups) + ['promo_period', 'responder']
    cols_to_keep = metrics + ['total']

    if 'reactivation_rate' in metrics:
        cols_to_keep += ['reactivations']
        data['reactivation_rate'] = ["{:.2%}".format(k)
            for k in data.reactivations/data.total]

    if 'activation_rate' in metrics:
        cols_to_keep += ['new_activations']
        data['activation_rate'] = ["{:.2%}".format(k)
            for k in data.new_activations/data.total]

    if 'cancelation_rate' in metrics:
        cols_to_keep += ['cancelations']
        data['cancelation_rate'] = ["{:.2%}".format(k)
            for k in data.cancelations/data.total]

    if 'avg_boxes_ordered' in metrics:
        cols_to_keep += ['total_boxes_ordered']
        data['avg_boxes_ordered'] = ["{:.2}".format(k)
            for k in data.total_boxes_ordered/data.total]

    if 'aov' in metrics:
        cols_to_keep += ['total_boxes_ordered', 'gov']
        data['aov'] = ["${:.2f}".format(k)
            for k in data.gov/data.total_boxes_ordered]
        data['gov'] = ["${:,.2f}".format(k)
            for k in data.gov]

    if 'dessert_take_rate' in metrics:
        cols_to_keep += ['total_boxes_ordered',
            'desserts_ordered']
        data['dessert_take_rate'] = ["{:.2%}".format(k)
            for k in data.desserts_ordered/data.total_boxes_ordered]

    cols_to_keep = groups + list(set(cols_to_keep))
    data = data[cols_to_keep]
    data.to_csv(path, index = False)

def main(args):
    logging.basicConfig(
        level=logging.INFO,
        format = '{asctime} {name:12s} {levelname:8s} {message}',
        datefmt = '%m-%d %H:%M:%S',
        style = '{' )

    reporting_options = yaml.load(open(args[0]),
        Loader = yamlordereddictloader.Loader)

    logging.info("Reporting options read from {}".format(
        os.path.abspath(args[0])))

    import_options_path = os.path.join(os.getenv('HOME'),
        *reporting_options['upload_template'])

    import_options = yaml.load(open(import_options_path),
        Loader = yamlordereddictloader.Loader)

    logging.info("Import options read from {}".format(import_options_path))

    schema_prefix = 'analytics.'
    table_name = import_options['table_name'] if \
        import_options['table_name'].startswith(schema_prefix) else \
        schema_prefix + import_options['table_name']

    path = os.path.dirname(os.path.abspath(args[0]))
    query = build_query(reporting_options, table_name,
        import_options['groups'].keys())

    with open(os.path.join(path, 'generated_query.sql'), 'w') as f:
        f.write(query)

    engine = create_engine("{driver}://{host}:{port}/{dbname}".format(
          driver = "postgresql+psycopg2",
          host = "localhost",
          port = 5439,
          dbname = "production"))

    if not engine.has_table(table_name.replace(schema_prefix, ''),
            schema = 'analytics'):
        sys.path.insert(0, os.path.join(os.getenv('HOME'),
            'analytics', 'projects'))
        from marketing_campaign_parse import import_marketing_to_redshift as im
        im.main(import_options_path)

    campaign_lists = Table(table_name.replace(schema_prefix, ''),
        MetaData(), schema = 'analytics', autoload_with = engine)
    if 'user_id' not in campaign_lists.columns:
        print(campaign_lists.columns)
        update_campaign_table(engine, table_name, campaign_lists)

    data = pd.read_sql_query(query, engine)
    compute_and_output_metrics(data, reporting_options['metrics'],
        path = os.path.join(path, os.path.basename(path) + '_report_metrics.csv'),
        groups = import_options['groups'].keys())

if __name__ == '__main__':
    main(sys.argv[1:])
