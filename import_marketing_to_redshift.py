import boto3
import pandas as pd
import os, re, sys
from pathlib import Path
from io import StringIO
from sqlalchemy import create_engine
from pandas.io.sql import get_schema
import yaml, logging

def read_to_pandas( options ):
    """
    Takes in a dictionary specifying how the data should be read in and
    returns a Pandas dataframe combining all input files with column(s)
    identifying which group(s) the data originated from.

    Args:
        options (dict): A dictionary containing at least following three keys:
            data_files: a list giving the directories that make up the path
            to the root directory of the data files. All live under HOME
            groups: a dictionary containing the column names and regular
            expressions to extract the value of that column for each (sub)group
            include_cols: a list containing the names of the columns to extract
            from all data files. Columns should exist in all files.

    Returns:
        pd.DataFrame: a dataframe containing the columns in include_cols and
        the columns in group_cols, with all data combined across files
    """
    campaign_path = Path(os.getenv('HOME'),
        *options['data_files'])

    # Search recursively for all files ending in *.csv no matter how many
    # levels down from root campaign path
    # all_files gives a list of absolute paths to all csv/Excel files in the directory
    all_files = [str(file) for file in list(campaign_path.glob('**/*.csv')) +
                                       list(campaign_path.glob('**/*.xlsx?'))]

    # nested dictionary
    # outer key = absolute path to file
    # value: {key = desired column name, value = substring matching regex}
    files_by_group = {x: {colname: re.search(pattern, x, re.IGNORECASE).group(0)
                for colname, pattern in options['groups'].items()}
        for x in all_files}

    all_data = pd.concat( process_data(path, group_cols,
                                       options['include_cols'])
                          for path, group_cols in files_by_group.items() )
    logging.info("Dataframe created from {n_files} files " \
                 "with {n_records} records in {n_cols} columns".format(
                 n_files = len(all_files),
                 n_records = all_data.shape[0],
                 n_cols = all_data.shape[1]))
    return all_data


def process_data( path, group_cols, include_cols ):
    """
    Reads in a single data file from disk processing only specified columns and
    adding in column(s) for (sub)groups to indicate which file the data
    originated from (single string value for all rows within a single file).

    Args:
        path (str): full absolute path to the csv/Excel file to be read in
        group_cols (dict): a dictionary where the keys are desired column names
        and the values are strings to set the value of each column to
        include_cols (list): a list of strings giving the column names to be
        read in from the original data file

    Returns:
        pd.DataFrame: a dataframe containing the columns in include_cols and
        the columns in group_cols, for a single file given by path
    """
    if path.endswith('csv'):
        try:
            data = pd.read_csv(path)
        except UnicodeDecodeError:
            data = pd.read_csv(path, encoding = 'iso-8859-1')
    else:
        data = pd.read_excel(path)
    data.columns = [col.lower().replace(' ', '_') for col in data.columns]
    include_cols = {col if type(col) == str
                        else next(x.group(0)
                            for x in [re.search(col[1], c)
                            for c in data.columns] if x)
                            :
                    col if type(col) == str else col[0]
        for col in include_cols}

    data = data[list(include_cols.keys())].rename(columns = include_cols)
    group_cols = {k.lower().replace(' ', '_'):
        v.lower().replace(' ', '_') for k,v in group_cols.items()}
    data = data.assign(**group_cols)
    logging.info("{} processed".format(path))
    return data


def upload_to_s3( data, options ):
    """
    Writes a Pandas DataFrame to CSV format and uploads to S3 in the specified
    bucket and location.

    Args:
        data (pd.DataFrame): dataframe to upload to s3
        options (dict): A dictionary containing at least following two keys:
            s3_location: specified bucket, folder, and filename to write to
            s3_readwrite_path: specifies where to find or clone the plated-airflow
            repo with helper_classes/S3ReadWrite.py

    Returns:
        str: name of the bucket where file has been written to
        str: full path (folders, filename and extension) to the file from
        within the s3 bucket
    """
    os.environ['S3_AIRFLOW_BUCKET'] = options['s3_location']['bucket']
    # Don't use Github version of S3ReadWrite for now
    #import_from_repo(options['s3_readwrite_path'])
    #from helper_classes import S3ReadWrite as s3rw
    import S3ReadWrite as s3rw
    s3_writer = s3rw.S3ReadWrite(str(Path(
        *options['s3_location']['folder'] )))

    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index = False, header = True)
    filename = options['s3_location']['filename']
    filename = filename if filename.endswith('.csv') else filename + '.csv'
    fullpath = str(Path( s3_writer.folder, filename))
    s3_writer.resource.Bucket(s3_writer.bucket).put_object(
        Key = fullpath, Body = csv_buffer.getvalue())
    logging.info("Data uploaded to {folder} in s3 bucket {bucket}".format(
        folder = fullpath, bucket = s3_writer.bucket))
    return s3_writer.bucket, fullpath


def import_from_repo( rw_path,
        clone_link = 'git@github.com:plated/plated-airflow.git' ):
    """
    Clones the plated-airflow repo if it does not already exist in the specified
    location, otherwise pulls the repo to get latest changes

    Args:
        rw_path (list): a list giving the directories that make up the path
        to the helper_classes directory of the repository
        clone_link (str): link to clone the repo (default is via SSH)
    """
    rw_path = Path(os.getenv('HOME'), *rw_path)
    repo_path = str(Path(*[part
        for i, part in enumerate(rw_path.parts)
        if i <= rw_path.parts.index('plated-airflow')]))

    if rw_path.is_dir():
        os.system("git -C {} pull".format(repo_path))
    else:
        os.system("git clone {} {}".format(clone_link, repo_path))

    sys.path.insert(0, str(rw_path))


def upload_to_redshift( bucket, filename, options, engine, data,
        iam = 308127741254, role = 'RedshiftCopy'):
    """
    Uploads a CSV file in S3 to a new table in Redshift. Creates table,
    copies data, and grants select privileges on the table.

    Args:
        bucket (str): name of bucket in S3 where data is stored
        filename (str): full path to file with data within S3 bucket
        options (dict): A dictionary containing at least following two keys:
            table_name: name of table to create (either bare or analytics.*)
            username_privileges: list of usernames to grant select privileges to
        engine (sqlalchemy.Engine): writeable engine to the database
        data (pd.DataFrame): data that was written to CSV containing dtype
        information for all columns
        iam (int): number for IAM role ARN to copy from S3 to Redshift
        role (str): role for IAM role ARN to copy from S3 to Redshift
    """
    iam_role = 'arn:aws:iam::{iam}:role/{role}'.format(
        iam = iam , role = role)

    tbl = options['table_name']
    tbl = tbl if tbl.startswith('analytics.') else 'analytics.' + tbl
    create_table_query = get_schema(data, tbl,
        con = engine).replace('"{}"'.format(tbl), tbl)

    copy_data_query = """ COPY {table_name}
    FROM 's3://{bucket}/{filename}'
         iam_role '{iam_role}'
         CSV BLANKSASNULL IGNOREHEADER AS 1 COMPUPDATE ON TIMEFORMAT 'auto'
         FILLRECORD STATUPDATE ON""".format(
         table_name = tbl,
         bucket = bucket,
         filename = filename,
         iam_role = iam_role)

    grant_privilege_queries = ["GRANT SELECT ON TABLE {table} to {user}".format(
        table = tbl, user = username )
        for username in options['username_privileges']]

    with engine.begin() as connection:
        connection.execute(create_table_query)
        logging.info('Created empty table {}'.format(tbl))

        connection.execute(copy_data_query)
        logging.info("Data copied from s3://{bucket}/{filename} to {table}".format(
            bucket = bucket, filename = filename, table = tbl))

        [connection.execute(grant_select)
            for grant_select in grant_privilege_queries]
        logging.info("SELECT privileges granted to {}".format(
            " ,".join(options['username_privileges'])))


def main(args):
    logging.basicConfig(
        level=logging.INFO,
        format = '{asctime} {name:12s} {levelname:8s} {message}',
        datefmt = '%m-%d %H:%M:%S',
        style = '{' )

    with open(args[1]) as f:
        import_options = yaml.load(f)

    engine = create_engine("{driver}://{host}:{port}/{dbname}".format(
          driver = "postgresql+psycopg2",
          host = "localhost",
          port = 5439,
          dbname = "production"))

    all_data = read_to_pandas( import_options )
    bucket, filename = upload_to_s3( all_data, import_options )
    upload_to_redshift( bucket, filename, import_options, engine, all_data )


if __name__ == '__main__':
    main(sys.argv)
