# -*- coding: utf-8 -*-
import argparse
import logging
from os import getenv
from os.path import join, expanduser

from hdx.facades.keyword_arguments import facade
from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging

from utilities.jsonoutput import jsonoutput
from model.main import get_indicators
from utilities.googlesheets import googlesheets

setup_logging()
logger = logging.getLogger()


VERSION = 2.0


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-ua', '--user_agent', default=None, help='user agent')
    parser.add_argument('-pp', '--preprefix', default=None, help='preprefix')
    parser.add_argument('-hs', '--hdx_site', default=None, help='HDX site to use')
    parser.add_argument('-gs', '--gsheet_auth', default=None, help='Credentials for accessing Google Sheets')
    parser.add_argument('-us', '--updatespreadsheets', default=None, help='Spreadsheets to update')
    parser.add_argument('-sc', '--scrapers', default=None, help='Scrapers to run')
    parser.add_argument('-ut', '--updatetabs', default=None, help='Sheets to update')
    parser.add_argument('-nj', '--nojson', default=False, action='store_true', help='Do not update json')
    args = parser.parse_args()
    return args


def main(gsheet_auth, updatesheets, updatetabs, scrapers, nojson, **ignore):
    logger.info('##### hdx-scraper-covid-viz version %.1f ####' % VERSION)
    configuration = Configuration.read()
    with Download(extra_params_yaml=join(expanduser('~'), '.extraparams.yml'), extra_params_lookup='hdx-scraper-fts', rate_limit={'calls': 1, 'period': 1}) as downloader:
        if scrapers:
            logger.info('Updating only scrapers: %s' % scrapers)
        tabs = configuration['tabs']
        if updatetabs is None:
            updatetabs = list(tabs.keys())
            logger.info('Updating all tabs')
        else:
            logger.info('Updating only these tabs: %s' % updatetabs)
        gsheets = googlesheets(configuration, gsheet_auth, updatesheets, tabs, updatetabs)
        jsonout = jsonoutput(configuration, updatetabs)
        get_indicators(configuration, downloader, gsheets, jsonout, updatetabs, scrapers)
        if not nojson:
            jsonout.add_additional_json(downloader)
            jsonout.save()


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
    updatesheets = args.updatespreadsheets
    if updatesheets is None:
        updatesheets = getenv('UPDATESHEETS')
    if updatesheets:
        updatesheets = updatesheets.split(',')
    else:
        updatesheets = None
    if args.updatetabs:
        updatetabs = args.updatetabs.split(',')
    else:
        updatetabs = None
    if args.scrapers:
        scrapers = args.scrapers.split(',')
    else:
        scrapers = None
    facade(main, hdx_read_only=True, user_agent=user_agent, preprefix=preprefix, hdx_site=hdx_site,
           project_config_yaml=join('config', 'project_configuration.yml'), gsheet_auth=gsheet_auth,
           updatesheets=updatesheets, updatetabs=updatetabs, scrapers=scrapers, nojson=args.nojson)
