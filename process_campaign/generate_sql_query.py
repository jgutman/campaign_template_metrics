import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table
import logging, re, os, sys
from datetime import datetime
from argparse import ArgumentParser
from process_campaign.upload_redshift import extract_campaign_info


def build_query(info):
    tbl_name = 'analytics.{}'.format(info.campaign_short_name.strip().lower())
    


def main(args):
    logging.basicConfig(
        level=logging.INFO,
        format = '{asctime} {name:12s} {levelname:8s} {message}',
        datefmt = '%m-%d %H:%M:%S',
        style = '{' )

    {logging.info('Input argument {} set to {}'.format(k, v)
        for k,v in vars(args).items()}

    campaign_dir, test_matrix, campaign_info = extract_campaign_info(args)
    query = build_query(campaign_info)

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
