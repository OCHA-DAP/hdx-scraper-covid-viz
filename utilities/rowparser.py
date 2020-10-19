# -*- coding: utf-8 -*-
import copy
from datetime import datetime

import hxl
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add

from model import template
from utilities.admininfo import AdminInfo


class RowParser(object):
    def __init__(self, level, datasetinfo, headers, indicatorcols, maxdateonly=True):
        if isinstance(level, str):
            if level == 'global':
                level = None
            elif level == 'national':
                level = 0
            else:
                level = 1
        self.level = level
        self.datecol = datasetinfo.get('date_col')
        self.datetype = datasetinfo.get('date_type')
        if self.datetype:
            if self.datetype == 'date':
                date = parse_date('1900-01-01')
            else:
                date = 0
        else:
            date = 0
        self.maxdate = date
        date_condition = datasetinfo.get('date_condition')
        if date_condition is not None:
            for col in datasetinfo['val_cols']:
                date_condition = date_condition.replace(col, f"row['{col}']")
        self.date_condition = date_condition
        self.admininfo = AdminInfo.get()
        self.admcols = datasetinfo.get('adm_cols', list())
        self.admexact = datasetinfo.get('adm_exact', False)
        self.indicatorcols = indicatorcols
        if self.level is None:
            self.maxdates = {i: date for i, _ in enumerate(indicatorcols)}
        else:
            if self.level > len(self.admcols):
                raise ValueError('No admin columns specified for required level!')
            self.maxdates = {i: {adm: date for adm in self.admininfo.adms[self.level]} for i, _ in enumerate(indicatorcols)}

        self.maxdateonly = maxdateonly
        self.flatteninfo = datasetinfo.get('flatten')
        self.headers = headers
        self.filters = dict()
        self.get_external_filter(datasetinfo)

    def get_external_filter(self, datasetinfo):
        external_filter = datasetinfo.get('external_filter')
        if not external_filter:
            return
        hxltags = external_filter['hxltags']
        data = hxl.data(external_filter['url'])
        for row in data:
            for hxltag in data.columns:
                if hxltag.display_tag in hxltags:
                    dict_of_lists_add(self.filters, hxltag.header, row.get('#country+code'))

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
        return self.maxdate

    def filtered(self, row):
        for header in self.filters:
            if header not in row:
                continue
            if row[header] not in self.filters[header]:
                return True
        return False

    def do_set_value(self, row, scrapername=None):
        if self.filtered(row):
            return None, None

        adms = [None for _ in range(len(self.admcols))]

        def get_adm(admcol, i):
            match = template.search(admcol)
            if match:
                template_string = match.group()
                admcol = self.headers[int(template_string[2:-2])]
            adm = row[admcol]
            if not adm:
                return False
            adms[i] = row[admcol].strip()
            return self.admininfo.get_adm(adms, self.admexact, i, scrapername)

        for i, admcol in enumerate(self.admcols):
            if admcol is None:
                continue
            if isinstance(admcol, str):
                admcol = [admcol]
            for admcl in admcol:
                exact = get_adm(admcl, i)
                if adms[i] and exact:
                    break
            if not adms[i]:
                return None, None

        indicators_process = list()
        for indicatorcol in self.indicatorcols:
            filtercol = indicatorcol['filter_col']
            process = True
            if filtercol:
                filtercols = filtercol.split('|')
                match = True
                for filterstr in filtercols:
                    filter = filterstr.split('=')
                    if row[filter[0]] != filter[1]:
                        match = False
                        break
                process = match
            indicators_process.append(process)

        if self.datecol:
            if isinstance(self.datecol, list):
                dates = [str(row[x]) for x in self.datecol]
                date = ''.join(dates)
            else:
                date = row[self.datecol]
            if self.datetype == 'date':
                if not isinstance(date, datetime):
                    date = parse_date(date)
                date = date.replace(tzinfo=None)
            else:
                date = int(date)
            if self.date_condition:
                if eval(self.date_condition) is False:
                    return None, None
            for i, process in enumerate(indicators_process):
                if not process:
                    continue
                if date > self.maxdate:
                    self.maxdate = date
                if self.level is None:
                    if self.maxdateonly:
                        if date < self.maxdates[i]:
                            indicators_process[i] = False
                        else:
                            self.maxdates[i] = date
                    else:
                        self.maxdates[i] = date
                else:
                    if self.maxdateonly:
                        if date < self.maxdates[i][adms[self.level]]:
                            indicators_process[i] = False
                        else:
                            self.maxdates[i][adms[self.level]] = date
                    else:
                        self.maxdates[i][adms[self.level]] = date
        if self.level is None:
            return 'global', indicators_process
        return adms[self.level], indicators_process
