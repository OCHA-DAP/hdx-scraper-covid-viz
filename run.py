# -*- coding: utf-8 -*-
import argparse
import json
import logging
from os import getenv
from os.path import join, expanduser

import pygsheets
from google.oauth2 import service_account

from hdx.facades.keyword_arguments import facade
from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging

from model.main import get_indicators

setup_logging()
logger = logging.getLogger()


VERSION = 1.0


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-ua', '--user_agent', default=None, help='user agent')
    parser.add_argument('-pp', '--preprefix', default=None, help='preprefix')
    parser.add_argument('-hs', '--hdx_site', default=None, help='HDX site to use')
    parser.add_argument('-gs', '--gsheet_auth', default=None, help='Credentials for accessing Google Sheets')
    parser.add_argument('--test', action='store_true', help='Whether to output to test')
    parser.add_argument('--scratch', action='store_true', help='Whether to output to scratch')
    parser.add_argument('-sc', '--scraper', default=None, help='Scraper to run')
    parser.add_argument('-us', '--updatesheets', default=None, help='Sheets to update')
    args = parser.parse_args()
    return args


def main(gsheet_auth, test, scratch, scraper, updatesheets, **ignore):
    logger.info('##### hdx-scraper-covid-viz version %.1f ####' % VERSION)
    configuration = Configuration.read()
    with Download(extra_params_yaml=join(expanduser('~'), '.extraparams.yml'), extra_params_lookup='hdx-scraper-fts', rate_limit={'calls': 1, 'period': 1}) as downloader:
        sheets = configuration['sheets']
        if updatesheets is None:
            updatesheets = sheets.keys()
            logger.info('Updating all sheets')
        else:
            logger.info('Updating only these sheets: %s' % updatesheets)
        if scraper:
            logger.info('Updating only scraper: %s' % scraper)
        world, national, nationaltimeseries, subnational, sources = get_indicators(configuration, downloader, updatesheets, scraper)
        # Write to gsheets
        info = json.loads(gsheet_auth)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        gc = pygsheets.authorize(custom_credentials=credentials)
        if test:
            url = configuration['test_spreadsheet_url']
        elif scratch:
            url = configuration['scratch_spreadsheet_url']
        else:
            url = configuration['prod_spreadsheet_url']
        spreadsheet = gc.open_by_url(url)

        def update_sheet(sheetname, values):
            if sheetname not in updatesheets:
                return
            sheet = spreadsheet.worksheet_by_title(sheets[sheetname])
            sheet.clear(fields='*')
            sheet.update_values('A1', values)

        update_sheet('world', world)
        update_sheet('national', national)
        update_sheet('national_timeseries', nationaltimeseries)
        update_sheet('subnational', subnational)
        update_sheet('sources', sources)


if __name__ == '__main__':
    args = parse_args()
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = getenv('USER_AGENT')
        if user_agent is None:
            user_agent = 'test'
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = getenv('PREPREFIX')
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = getenv('HDX_SITE', 'prod')
    gsheet_auth = args.gsheet_auth
    if gsheet_auth is None:
        gsheet_auth = getenv('GSHEET_AUTH')
    if args.updatesheets:
        updatesheets = args.updatesheets.split(',')
    else:
        updatesheets = None
    facade(main, hdx_read_only=True, user_agent=user_agent, preprefix=preprefix, hdx_site=hdx_site,
           project_config_yaml=join('config', 'project_configuration.yml'), gsheet_auth=gsheet_auth, test=args.test,
           scratch=args.scratch, scraper=args.scraper, updatesheets=updatesheets)
