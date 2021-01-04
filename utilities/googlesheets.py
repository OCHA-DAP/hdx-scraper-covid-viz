# -*- coding: utf-8 -*-
import json
import logging

import pygsheets
from google.oauth2 import service_account

logger = logging.getLogger()


class googlesheets:
    def __init__(self, configuration, gsheet_auth, updatesheets, tabs, updatetabs):
        info = json.loads(gsheet_auth)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        self.gc = pygsheets.authorize(custom_credentials=credentials)
        self.googlesheets = configuration['googlesheets']
        if updatesheets is None:
            updatesheets = self.googlesheets.keys()
            logger.info('Updating all spreadsheets')
        else:
            logger.info('Updating only these spreadsheets: %s' % updatesheets)
        self.updatesheets = updatesheets
        self.tabs = tabs
        self.updatetabs = updatetabs

    def update_tab(self, tabname, values, hxltags=None, limit=None):
        if tabname not in self.updatetabs:
            return
        for sheet in self.googlesheets:
            if sheet not in self.updatesheets:
                continue
            url = self.googlesheets[sheet]
            spreadsheet = self.gc.open_by_url(url)

            tab = spreadsheet.worksheet_by_title(self.tabs[tabname])
            tab.clear(fields='*')
            if isinstance(values, list):
                tab.update_values('A1', values)
            else:
                headers = list(values.columns.values)
                tab.update_row(1, headers)
                if hxltags:
                    tab.update_row(2, [hxltags.get(header, '') for header in headers])
                    start = 3
                else:
                    start = 2
                if limit is not None:
                    values = values.head(limit)
                tab.set_dataframe(values, (start, 1), copy_head=False)
