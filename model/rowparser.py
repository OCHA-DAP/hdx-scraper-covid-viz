# -*- coding: utf-8 -*-
import copy
from datetime import datetime

from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date

from model import template
from model.admininfo import AdminInfo


class RowParser(object):
    def __init__(self, adms, datasetinfo, headers, maxdateonly=True):
        self.adms = adms
        self.admcols = datasetinfo['adm_cols']
        self.datecol = datasetinfo.get('date_col')
        self.datetype = datasetinfo.get('date_type')
        if self.datetype:
            if self.datetype == 'date':
                date = parse_date('1900-01-01')
            else:
                date = 0
        else:
            date = 0
        self.maxdates = {adm: date for adm in adms[-1]}
        self.maxdateonly = maxdateonly
        self.flatteninfo = datasetinfo.get('flatten')
        self.headers = headers
        self.adm_mappings = datasetinfo['adm_mappings']

    def flatten(self, row):
        if not self.flatteninfo:
            yield row
            return
        counters = [-1 for _ in self.flatteninfo]
        while True:
            newrow = copy.deepcopy(row)
            for i, flatten in enumerate(self.flatteninfo):
                colname = flatten['original']
                match = template.search(colname)
                if not match:
                    raise ValueError('Column name for flattening lacks an incrementing number!')
                template_string = match.group()
                if counters[i] == -1:
                    replace_string = template_string[2:-2]
                    counters[i] = int(replace_string)
                else:
                    replace_string = '%d' % counters[i]
                colname = colname.replace(template_string, replace_string)
                if colname not in row:
                    return
                newrow[flatten['new']] = row[colname]
                extracol = flatten.get('extracol')
                if extracol:
                    newrow[extracol] = colname
                counters[i] += 1
            yield newrow

    def get_maxdate(self):
        return max(self.maxdates.values())

    def do_set_value(self, row, scrapername=None):
        adm = None

        def get_adm(admcol, prev_adm):
            match = template.search(admcol)
            if match:
                template_string = match.group()
                admcol = self.headers[int(template_string[2:-2])]
            adm = row[admcol]
            if not adm:
                return None
            if adm not in self.adms[i]:
                mapped_adm = self.adm_mappings[i].get(adm)
                if mapped_adm:
                    return mapped_adm
                if i == 0:
                    adm, _ = Country.get_iso3_country_code_fuzzy(adm)
                elif i == 1:
                    adm = AdminInfo.get().get_pcode(prev_adm, adm, scrapername)
                else:
                    return None
                if adm not in self.adms[i]:
                    return None
            return adm

        for i, admcol in enumerate(self.admcols):
            if admcol is None:
                continue
            prev_adm = adm
            if isinstance(admcol, str):
                admcol = [admcol]
            for admcl in admcol:
                adm = get_adm(admcl, prev_adm)
                if adm:
                    break
            if not adm:
                return None, None
        if self.datecol:
            date = row[self.datecol]
            if self.datetype == 'int':
                date = int(date)
            else:
                if not isinstance(date, datetime):
                    date = parse_date(date)
                date = date.replace(tzinfo=None)
            if self.maxdateonly:
                if date < self.maxdates[adm]:
                    return None, None
            self.maxdates[adm] = date
        else:
            date = None
        return adm, date