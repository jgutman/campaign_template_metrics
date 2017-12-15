import boto3
import pandas as pd
import os, re, sys
from pathlib import Path
from io import StringIO
from sqlalchemy import create_engine
from pandas.io.sql import get_schema
import logging
from argparse import ArgumentParser
from s3_read_write import S3ReadWrite


def process_send_lists(files, test_matrix):
    # Search recursively for all files ending in *.csv, *.xls, *.xlsx
    all_files = [str(file) for file in list(campaign_dir.glob('**/*.csv')) +
                            list(campaign_dir.glob('**/*.xlsx?'))]

    targets_by_file = {str(f): target_name
        for target_name in test_matrix.target_name
        for f in campaign_dir.glob('**/{}*'.format(target_name))}

    data = pd.concat([process_single_file(f, target)
        for f, target in targets_by_file.items()])

    data = data.join(test_matrix, on = 'target_name')
    return data

def process_single_file(path, target):
    if path.endswith('csv'):
        try:
            data = pd.read_csv(path,
                usecols = lambda x: x.lower().replace(' ', '_') \
                in ['user_id', 'internal_user_id', 'external_id',
                'prospect_id', 'email'])
        except UnicodeDecodeError:
            data = pd.read_csv(path,
                usecols = lambda x: x.lower().replace(' ', '_') \
                in ['user_id', 'internal_user_id', 'external_id',
                'prospect_id', 'email'],
                encoding = 'iso-8859-1')
    else:
        data = pd.read_excel(path).drop()
        keep_cols = [col for col in data.columns
            if col.lower().replace(' ', '_') \
            in ['user_id', 'internal_user_id', 'external_id',
            'prospect_id', 'email']]
        data = data[keep_cols]

    data.columns = [col.lower().replace(' ', '_')
        for col in data.columns]
    data['target_name'] = target
    return data


def upload_to_s3( data, bucket, s3dir, info ):
    s3_writer = S3ReadWrite(bucket = bucket, folder = s3dir)
    csv_path = info.campaign_name.strip()
    csv_name = info.campaign_short_name.strip().lower()
    s3_writer.put_dataframe_to_S3(csv_path = csv_path,
        csv_name = csv_name, dataframe = data)
    fullpath = str(Path(s3dir, csv_path, '{}.csv'.format(csv_name)))
    return fullpath, csv_name


def main(args):
    logging.basicConfig(
        level=logging.INFO,
        format = '{asctime} {name:12s} {levelname:8s} {message}',
        datefmt = '%m-%d %H:%M:%S',
        style = '{' )

    campaign_dir = Path(args.root_dir, args.campaign_dir).absolute()
    template_path =  [str(x) for x in campaign_dir.glob('**/*_template.xlsx')][0]
    template = pd.read_excel(template_path)

    test_matrix_cols = ['test_group', 'segment_group', 'offer_group',
        'target_name', 'creative_template_name', 'population_name',
        'offer_campaign_name', 'discount_name', 'message_offer']

    campaign_info = template.drop(columns = test_matrix_cols).iloc[0]

    data = process_send_lists(campaign_dir, template[test_matrix_cols])
    s3_path, table_name = upload_to_s3(data, args.bucket, args.s3dir,
        info = campaign_info)
    # upload from s3 to redshift

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
