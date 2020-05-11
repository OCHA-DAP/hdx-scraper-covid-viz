# -*- coding: utf-8 -*-
from datetime import datetime

from hdx.utilities.dateparse import parse_date


class RowParser(object):
    def __init__(self, countries, datasetinfo):
        self.countries = countries
        self.iso3col = datasetinfo['iso3_col']
        self.datecol = datasetinfo.get('date_col')
        self.datetype = datasetinfo.get('date_type')
        if self.datetype:
            if self.datetype == 'date':
                self.maxdate = parse_date('1900-01-01')
            else:
                self.maxdate = 0
        else:
            self.maxdate = 0
        self.filtercol = datasetinfo.get('filter_col')
        if self.filtercol:
            self.filtercol = self.filtercol.split('=')

    def do_set_value(self, row):
        countryiso = row[self.iso3col]
        if countryiso not in self.countries:
            return None
        if self.datecol:
            date = row[self.datecol]
            if self.datetype == 'int':
                date = int(date)
            else:
                if not isinstance(date, datetime):
                    date = parse_date(date)
                date = date.replace(tzinfo=None)
            if date < self.maxdate:
                return None
            self.maxdate = date
        if self.filtercol:
            if row[self.filtercol[0]] != self.filtercol[1]:
                return None
        return countryiso