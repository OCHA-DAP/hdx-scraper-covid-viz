# -*- coding: utf-8 -*-
from datetime import datetime

from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date

from model.admininfo import AdminInfo


class RowParser(object):
    def __init__(self, adms, datasetinfo):
        self.adms = adms
        self.admcols = datasetinfo['adm_cols']
        self.datecol = datasetinfo.get('date_col')
        self.datetype = datasetinfo.get('date_type')
        if self.datetype:
            if self.datetype == 'date':
                self.maxdate = parse_date('1900-01-01')
            else:
                self.maxdate = 0
        else:
            self.maxdate = 0

    def do_set_value(self, row):
        adm = None
        for i, admcol in enumerate(self.admcols):
            if admcol is None:
                continue
            prev_adm = adm
            adm = row[admcol]
            if not adm:
                return None
            if adm not in self.adms[i]:
                if i == 0:
                    adm, _ = Country.get_iso3_country_code_fuzzy(adm)
                elif i == 1:
                    adm = AdminInfo.get().get_pcode(prev_adm, adm)
                else:
                    return None
                if adm not in self.adms[i]:
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
        return adm