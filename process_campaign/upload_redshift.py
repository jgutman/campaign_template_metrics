import boto3
import pandas as pd
import sys
from pathlib import Path
from io import StringIO
from sqlalchemy import create_engine, text
from pandas.io.sql import get_schema
import logging
from argparse import ArgumentParser
from .s3_read_write import S3ReadWrite


def process_send_lists(campaign_dir, test_matrix):
    # Search recursively for all files ending in *.csv, *.xls, *.xlsx
    all_files = [str(file) for file in
        list(campaign_dir.glob('**/Send Lists/**/*.csv')) +
        list(campaign_dir.glob('**/Send Lists/**/*.xlsx'))]
    logging.info('{} files found'.format(len(all_files)))

    targets_by_file = {str(f): target_name
        for target_name in test_matrix.target_name
        for f in campaign_dir.glob('**/{}*'.format(target_name))}

    data = pd.concat([process_single_file(f, target)
        for f, target in targets_by_file.items()])

    data = (data.rename(columns = {'internal_user_id': 'user_id',
                                  'email_address': 'email'})
                .dropna(axis=1, how='all')
                .merge(test_matrix, on = 'target_name'))

    logging.info("Dataframe created from {n_files} files " \
                 "with {n_records} records in {n_cols} columns".format(
                 n_files = len(targets_by_file.keys()),
                 n_records = data.shape[0],
                 n_cols = data.shape[1]))
    return data

def process_single_file(path, target):
    id_cols = ['user_id', 'internal_user_id', 'prospect_id',
               'email', 'email_address']

    if path.endswith('csv'):
        try:
            data = pd.read_csv(path, header=0,
                usecols=lambda x: x.lower().replace(' ', '_') in id_cols)
        except UnicodeDecodeError:
            data = pd.read_csv(path, header=0,
                usecols=lambda x: x.lower().replace(' ', '_') in id_cols,
                encoding='iso-8859-1')
    else:
        data = pd.read_excel(path).drop()
        keep_cols = [col for col in data.columns
            if col.lower().replace(' ', '_') in id_cols]
        data = data[keep_cols]

    data.columns = [col.lower().replace(' ', '_')
        for col in data.columns]
    data['target_name'] = target
    logging.info('{} successfully processed'.format(path))
    return data


def upload_to_s3(data, bucket, s3dir, info):
    s3_writer = S3ReadWrite(bucket=bucket, folder=s3dir)
    logging.info('S3ReadWrite created in {}'.format(str(s3_writer)))
    csv_path = info.campaign_name.strip()
    csv_name = info.campaign_short_name.strip().lower()
    s3_writer.put_dataframe_to_S3(csv_path=csv_path,
                                  csv_name=csv_name, dataframe=data)
    fullpath = str(Path(s3dir, csv_path, '{}.csv'.format(csv_name)))
    logging.info('data saved in S3 bucket at {}'.format(fullpath))
    return fullpath, csv_name


def upload_to_redshift(bucket, filename, tbl_name, engine, data, usernames,
                       iam = 308127741254, role = 'RedshiftCopy'):
    iam_role = 'arn:aws:iam::{iam}:role/{role}'.format(iam=iam , role=role)

    tbl = tbl_name if tbl_name.startswith('analytics.') \
                   else 'analytics.{}'.format(tbl_name)
    create_table_query = (get_schema(data, tbl, con=engine)
                          .replace('"{}"'.format(tbl), tbl))

    copy_data_query = """ COPY {table_name}
    FROM 's3://{bucket}/{filename}'
         iam_role '{iam_role}'
         CSV BLANKSASNULL IGNOREHEADER AS 1 COMPUPDATE ON TIMEFORMAT 'auto'
         FILLRECORD STATUPDATE ON""".format(
         table_name = tbl,
         bucket = bucket,
         filename = filename,
         iam_role = iam_role)

    grant_privilege_queries = ["GRANT SELECT ON TABLE {table} to {user}"
                               .format(table=tbl, user=username )
                               for username in usernames]

    with engine.begin() as connection:
        connection.execute(create_table_query)
        logging.info('Created empty table {}'.format(tbl))
        connection.execute(copy_data_query)
        logging.info("Data copied from s3://{bucket}/{filename} to {table}".format(
            bucket = bucket, filename = filename, table = tbl))
        [connection.execute(grant_select)
            for grant_select in grant_privilege_queries]
        logging.info('SELECT privileges granted to {}'.format(
            " ,".join(usernames)))


def extract_campaign_info(args):
    campaign_dir = Path(args.root_dir, args.campaign_dir).absolute()
<<<<<<< HEAD
    template_path =  [str(x) for x in
        list(campaign_dir.glob('**/*_template.csv')) +
        list(campaign_dir.glob('**/*_template.xlsx'))][0]
    if template_path.endswith('csv'):
        template = pd.read_csv(template_path)
        date_cols = [col for col in template.columns if col.endswith('date')]
        template[date_cols] = template[date_cols].apply(
            lambda x: pd.to_datetime(x, infer_datetime_format = True))
    else:
        template = pd.read_excel(template_path)
=======
    template_path =  [str(x) for x in campaign_dir.glob('**/*_template.xlsx')][0]
    template = pd.read_excel(template_path)
>>>>>>> 5c8ff22dc686692cb028ebe56e0b74eed0382d69
    logging.info('Campaign template read from {}'.format(template_path))

    test_matrix_cols = ['test_group', 'segment_group', 'offer_group',
        'target_name', 'creative_template_name', 'population_name',
        'offer_campaign_name', 'discount_name', 'message_offer']

    campaign_info = template.drop(columns = test_matrix_cols).iloc[0]
    return campaign_dir, template[test_matrix_cols], campaign_info


def replace_table(data, engine, args, usernames, campaign_info):
    tbl_name = campaign_info.campaign_short_name
    drop_table = "DROP TABLE IF EXISTS analytics.{}".format(tbl_name)

    with engine.begin() as connection:
        connection.execute(drop_table)
    logging.info('Table analytics.{} dropped'.format(tbl_name))

    s3_path, tbl_name = upload_to_s3(data, args.bucket, args.s3dir,
                                     info = campaign_info)
    upload_to_redshift(args.bucket, s3_path,
        tbl_name = tbl_name, engine = engine,
        data = data, usernames = usernames)
    logging.info('Table analytics.{} replaced'.format(tbl_name))


def update_campaign_table(data, engine, args, usernames, campaign_info):
    tbl_name = campaign_info.campaign_short_name

    if 'user_id' in data.columns:
        logging.info('User_id column found as primary identifier')

    elif 'prospect_id' in data.columns:
        join_query = """SELECT a.*,
            u.internal_user_id AS user_id
        FROM analytics.{} a
        LEFT JOIN dw.users u
        ON u.internal_marketing_prospect_id = a.prospect_id
        AND u.internal_user_id is not null
        """.format(tbl_name)

        data = pd.read_sql_query(text(join_query), engine)
        replace_table(data, engine, args, usernames, campaign_info)
        logging.info('Joined on marketing_prospect_id and added user_id')

    elif 'external_id' in data.columns:
        join_query = """SELECT a.*,
            u.internal_user_id AS user_id
        FROM analytics.{} a
        LEFT JOIN dw.users u
        ON u.external_id = a.external_id
        AND u.internal_user_id is not null
        """.format(tbl_name)

        data = pd.read_sql_query(text(join_query), engine)
        replace_table(data, engine, args, usernames, campaign_info)
        logging.info('Joined on external_id and added user_id')

    elif 'email' in data.columns:
        join_query = """SELECT a.*,
            u.internal_user_id AS user_id
        FROM analytics.{} a
        LEFT JOIN dw.users u
        ON u.email = a.email
        AND u.internal_user_id is not null
        """.format(tbl_name)

        data = pd.read_sql_query(text(join_query), engine)
        replace_table(data, engine, args, usernames, campaign_info)
        logging.info('Joined on user email address and added user_id')

    else:
        logging.error('Could not find non-null id column in {tbl}: {cols}'
        .format(tbl=tbl_name, cols=' '.join(['user_id', 'prospect_id',
                                             'external_id', 'email'])))


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

    # extract information from campaign template file
    campaign_dir, test_matrix, campaign_info = extract_campaign_info(args)
    # concatenate all send lists into DataFrame
    data = process_send_lists(campaign_dir, test_matrix)

    usernames = ['production_read_only', 'analytics_team', 'prod_application']
    # connect to database via Strong DM
    engine = create_engine('{driver}://{host}:{port}/{dbname}'.format(
          driver = 'postgresql+psycopg2',
          host = 'localhost',
          port = 5439,
          dbname = 'production'))

    # upload csv of concatenated send lists to S3
    # copy data from S3 to new table in Redshift
    replace_table(data, engine, args, usernames, campaign_info)

    update_campaign_table(data, engine, args, usernames, campaign_info)

    logging.info('Database upload completed successfully for {}'
                 .format(args.campaign_dir))

if __name__ == '__main__':
    parser = ArgumentParser('Upload campaign lists to Redshift.')
    parser.add_argument('--root_dir',
        help = 'path to directory containing information on all campaigns',
        default = str(Path(Path.home(),
            'Google Drive File Stream', 'My Drive',
            'Reformatted Prioritized Campaign Lists')))
    parser.add_argument('campaign_dir',
        help = 'name of directory for desired campaign')
    parser.add_argument('--bucket',
        help = 'S3 bucket to save send lists',
        default = 'plated-redshift-etl')
    parser.add_argument('--s3dir',
        help = 'S3 directory within bucket to save send lists',
        default = 'manual/campaigns_jackie')
    args = parser.parse_args()
    main(args)
