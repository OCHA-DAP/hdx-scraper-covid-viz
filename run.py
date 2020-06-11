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
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.saver import save_json

from model.additional_json import add_additional_json
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
    parser.add_argument('-us', '--updatespreadsheets', default=None, help='Spreadsheets to update')
    parser.add_argument('-sc', '--scraper', default=None, help='Scraper to run')
    parser.add_argument('-ut', '--updatetabs', default=None, help='Sheets to update')
    args = parser.parse_args()
    return args


def generate_json(json, key, rows):
    hxltags = rows[1]
    for row in rows[2:]:
        newrow = dict()
        for i, hxltag in enumerate(hxltags):
            value = row[i]
            if value is None:
                value = ''
            newrow[hxltag] = str(value)
        dict_of_lists_add(json, key, newrow)


def write_json(configuration, downloader, updatetabs, world, national, nationaltimeseries, subnational, sources):
    json = dict()

    def update_json(tabname, values):
        if tabname not in updatetabs:
            return
        generate_json(json, '%s_data' % tabname, values)

    update_json('world', world)
    update_json('national', national)
    update_json('national_timeseries', nationaltimeseries)
    update_json('subnational', subnational)
    update_json('sources', sources)
    add_additional_json(configuration, downloader, json)
    save_json(json, 'out.json')


def write_to_gsheets(spreadsheets, updatesheets, tabs, updatetabs, world, national, nationaltimeseries, subnational, sources):
    # Write to gsheets
    info = json.loads(gsheet_auth)
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    gc = pygsheets.authorize(custom_credentials=credentials)
    for sheet in spreadsheets:
        if sheet not in updatesheets:
            continue
        url = spreadsheets[sheet]
        spreadsheet = gc.open_by_url(url)

        def update_tab(tabname, values):
            if tabname not in updatetabs:
                return
            tab = spreadsheet.worksheet_by_title(tabs[tabname])
            tab.clear(fields='*')
            tab.update_values('A1', values)

        update_tab('world', world)
        update_tab('national', national)
        update_tab('national_timeseries', nationaltimeseries)
        update_tab('subnational', subnational)
        update_tab('sources', sources)


def main(gsheet_auth, updatesheets, updatetabs, scraper, **ignore):
    logger.info('##### hdx-scraper-covid-viz version %.1f ####' % VERSION)
    configuration = Configuration.read()
    with Download(extra_params_yaml=join(expanduser('~'), '.extraparams.yml'), extra_params_lookup='hdx-scraper-fts', rate_limit={'calls': 1, 'period': 1}) as downloader:
        spreadsheets = configuration['spreadsheets']
        if updatesheets is None:
            updatesheets = spreadsheets.keys()
            logger.info('Updating all spreadsheets')
        else:
            logger.info('Updating only these spreadsheets: %s' % updatesheets)

        tabs = configuration['tabs']
        if updatetabs is None:
            updatetabs = list(tabs.keys())
            updatetabs.remove('national_timeseries')  ## REMOVING TIMESERIES for now
            logger.info('Updating all tabs')
        else:
            logger.info('Updating only these tabs: %s' % updatetabs)
        if scraper:
            logger.info('Updating only scraper: %s' % scraper)
        world, national, nationaltimeseries, subnational, sources = get_indicators(configuration, downloader, updatetabs, scraper)
        write_json(configuration, downloader, updatetabs, world, national, nationaltimeseries, subnational, sources)
        write_to_gsheets(spreadsheets, updatesheets, tabs, updatetabs, world, national, nationaltimeseries, subnational, sources)


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
    facade(main, hdx_read_only=True, user_agent=user_agent, preprefix=preprefix, hdx_site=hdx_site,
           project_config_yaml=join('config', 'project_configuration.yml'), gsheet_auth=gsheet_auth,
           updatesheets=updatesheets, updatetabs=updatetabs, scraper=args.scraper)
