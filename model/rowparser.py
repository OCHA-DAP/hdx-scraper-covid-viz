# -*- coding: utf-8 -*-
import copy
from datetime import datetime

from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date

from model import template
from model.admininfo import AdminInfo


class RowParser(object):
    levels = {'global': 0, 'national': 1, 'subnational': 2}

    def __init__(self, adms, level, datasetinfo, headers, maxdateonly=True):
        self.adms = adms
        self.level = self.levels[level]
        self.datecol = datasetinfo.get('date_col')
        self.datetype = datasetinfo.get('date_type')
        if self.datetype:
            if self.datetype == 'date':
                date = parse_date('1900-01-01')
            else:
                date = 0
        else:
            date = 0
        self.admcols = copy.deepcopy(datasetinfo.get('adm_cols', list()))
        if self.level > len(self.admcols):
            raise ValueError('No admin columns specified for required level!')
        self.admcols.insert(0, None)
        self.adm_mappings = copy.deepcopy(datasetinfo.get('adm_mappings', [dict(), dict()]))
        self.adm_mappings.insert(0, None)
        self.maxdates = {adm: date for adm in adms[self.level]}
        self.maxdateonly = maxdateonly
        self.flatteninfo = datasetinfo.get('flatten')
        self.headers = headers

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
        admininfo = AdminInfo.get()

        def get_adm(admcol, adms, i):
            match = template.search(admcol)
            if match:
                template_string = match.group()
                admcol = self.headers[int(template_string[2:-2])]
            adm = row[admcol]
            if not adm:
                return None, None
            exact = True
            if adm not in self.adms[i]:
                mapped_adm = self.adm_mappings[i].get(adm)
                if mapped_adm:
                    return mapped_adm, True
                if i == 1:
                    adm, _ = Country.get_iso3_country_code_fuzzy(adm)
                    exact = False
                elif i == 2:
                    pcode = admininfo.convert_pcode_length(adms[1], adm, scrapername)
                    if pcode:
                        adm = pcode
                        exact = True
                    else:
                        adm = admininfo.get_pcode(adms[1], adm, scrapername)
                        exact = False
                else:
                    return None, None
                if adm not in self.adms[i]:
                    return None, None
            return adm, exact

        adms = [None for _ in range(len(self.admcols))]
        adms[0] = 'global'
        for i, admcol in enumerate(self.admcols):
            if admcol is None:
                continue
            if isinstance(admcol, str):
                admcol = [admcol]
            for admcl in admcol:
                adms[i], exact = get_adm(admcl, adms, i)
                if adms[i] and exact:
                    break
            if not adms[i]:
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
                if date < self.maxdates[adms[self.level]]:
                    return None, None
            self.maxdates[adms[self.level]] = date
        else:
            date = None
        return adms[self.level], date
