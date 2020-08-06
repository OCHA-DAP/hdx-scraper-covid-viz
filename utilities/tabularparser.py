# -*- coding: utf-8 -*-
import logging
from datetime import datetime

import regex

from hdx.utilities.dateparse import parse_date, get_datetime_from_timestamp
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import number_format, get_fraction_str, get_numeric_if_possible

from model import today_str
from utilities import get_rowval
from utilities.rowparser import RowParser
from utilities.readers import read_tabular, read_ole, read_json, read_hdx

logger = logging.getLogger(__name__)

brackets = r'''
(?<rec> #capturing group rec
 \( #open parenthesis
 (?: #non-capturing group
  [^()]++ #anyting but parenthesis one or more times without backtracking
  | #or
   (?&rec) #recursive substitute of group rec
 )*
 \) #close parenthesis
)'''


def _get_tabular(level, name, datasetinfo, headers, iterator, retheaders=[list(), list()], retval=list(), sources=list()):
    rowparser = RowParser(level, datasetinfo, headers)
    indicatorcols = datasetinfo.get('indicator_cols')
    if not indicatorcols:
        indicatorcols = [{'filter_col': datasetinfo.get('filter_col'), 'val_cols': datasetinfo['val_cols'],
                          'val_fns': datasetinfo.get('val_fns', dict()), 'eval_cols':  datasetinfo.get('eval_cols', list()),
                          'keep_cols': datasetinfo.get('keep_cols', list()), 'append_cols': datasetinfo.get('append_cols', list()),
                          'total_col': datasetinfo.get('total_col'), 'ignore_vals': datasetinfo.get('ignore_vals', list()),
                          'columns': datasetinfo['columns'], 'hxltags': datasetinfo['hxltags']}]
    valuedicts = dict()
    for indicatorcol in indicatorcols:
        for _ in indicatorcol['val_cols']:
            dict_of_lists_add(valuedicts, indicatorcol['filter_col'], dict())

    def add_row(row):
        adm, _ = rowparser.do_set_value(row, name)
        if not adm:
            return
        for indicatorcol in indicatorcols:
            filtercol = indicatorcol['filter_col']
            if filtercol:
                filtercols = filtercol.split(',')
                match = True
                for filterstr in filtercols:
                    filter = filterstr.split('=')
                    if row[filter[0]] != filter[1]:
                        match = False
                        break
                if not match:
                    continue
            total_col = indicatorcol.get('total_col')
            eval_cols = indicatorcol.get('eval_cols')
            append_cols = indicatorcol.get('append_cols', list())
            keep_cols = indicatorcol.get('keep_cols', list())
            for i, valcol in enumerate(indicatorcol['val_cols']):
                valuedict = valuedicts[filtercol][i]
                val = get_rowval(row, valcol)
                if total_col or eval_cols:
                    dict_of_lists_add(valuedict, adm, val)
                else:
                    curval = valuedict.get(adm)
                    if valcol in append_cols:
                        if curval:
                            val = curval + val
                    elif valcol in keep_cols:
                        if curval:
                            val = curval
                    valuedict[adm] = val

    stop_row = datasetinfo.get('stop_row')
    for row in iterator:
        if not isinstance(row, dict):
            row = row.value
        if stop_row:
            if all(row[key] == value for key, value in stop_row.items()):
                break
        for newrow in rowparser.flatten(row):
            add_row(newrow)

    date = datasetinfo.get('date')
    use_date_from_date_col = datasetinfo.get('use_date_from_date_col', False)
    if date and not use_date_from_date_col:
        date = parse_date(date)
    else:
        date = rowparser.get_maxdate()
        if date == 0:
            raise ValueError('No date given in datasetinfo or as a column!')
        if rowparser.datetype == 'date':
            if not isinstance(date, datetime):
                date = parse_date(date)
        elif rowparser.datetype == 'int':
            date = get_datetime_from_timestamp(date)
        else:
            raise ValueError('No date type specified!')
    date = date.strftime('%Y-%m-%d')

    for indicatorcol in indicatorcols:
        retheaders[0].extend(indicatorcol['columns'])
        hxltags = indicatorcol['hxltags']
        retheaders[1].extend(hxltags)
        valdicts = valuedicts[indicatorcol['filter_col']]
        eval_cols = indicatorcol.get('eval_cols')
        keep_cols = indicatorcol.get('keep_cols')
        total_col = indicatorcol.get('total_col')
        ignore_vals = indicatorcol.get('ignore_vals', list())
        val_fns = indicatorcol.get('val_fns', dict())
        valcols = indicatorcol['val_cols']
        # Indices of list sorted by length
        sorted_len_indices = sorted(range(len(valcols)), key=lambda k: len(valcols[k]), reverse=True)
        if eval_cols:
            newvaldicts = [dict() for _ in eval_cols]

            def text_replacement(string, adm):
                hasvalues = False
                for j in sorted_len_indices:
                    valcol = valcols[j]
                    if valcol not in string:
                        continue
                    if valcol in keep_cols:
                        keep_col_index = 0
                    else:
                        keep_col_index = -1
                    val = valdicts[j][adm][keep_col_index]
                    if not val or val in ignore_vals:
                        val = 0
                    else:
                        val_fn = val_fns.get(valcol)
                        if val_fn:
                            val = eval(val_fn.replace(valcol, 'val'))
                        hasvalues = True
                    string = string.replace(valcol, str(val))
                return string, hasvalues

            for i, eval_col in enumerate(eval_cols):
                valdict0 = valdicts[0]
                for adm in valdict0:
                    hasvalues = True
                    matches = regex.search(brackets, eval_col, flags=regex.VERBOSE)
                    if matches:
                        for bracketed_str in matches.captures('rec'):
                            if any(bracketed_str in x for x in valcols):
                                continue
                            _, hasvalues_t = text_replacement(bracketed_str, adm)
                            if not hasvalues_t:
                                hasvalues = False
                                break
                    if hasvalues:
                        newvaldicts[i][adm], hasvalues_t = text_replacement(eval_col, adm)
                        if hasvalues_t:
                            newvaldicts[i][adm] = eval(newvaldicts[i][adm])
                        else:
                            newvaldicts[i][adm] = ''
                    else:
                        newvaldicts[i][adm] = ''
            retval.extend(newvaldicts)
        elif total_col:
            formula = total_col['formula']
            mustbepopulated = total_col.get('mustbepopulated', False)
            newvaldicts = [dict() for _ in valdicts]
            valdict0 = valdicts[0]
            for adm in valdict0:
                for i, val in enumerate(valdict0[adm]):
                    if not val or val in ignore_vals:
                        exists = False
                    else:
                        exists = True
                        for valdict in valdicts[1:]:
                            val = valdict[adm]
                            if not val or val in ignore_vals:
                                exists = False
                                break
                    if mustbepopulated and not exists:
                        continue
                    for j, valdict in enumerate(valdicts):
                        valcol = valcols[j]
                        val_fn = val_fns.get(valcol)
                        if not val_fn:
                            val_fn = valcol
                        newvaldicts[j][adm] = newvaldicts[j].get(adm, 0.0) + eval(val_fn.replace(valcol, 'valdict[adm][i]'))
            for i, valcol in enumerate(valcols):
                formula = formula.replace(valcol, 'newvaldicts[%d][adm]' % i)
            newvaldict = dict()
            for adm in valdicts[0].keys():
                try:
                    val = eval(formula)
                except (ValueError, TypeError, KeyError):
                    val = ''
                newvaldict[adm] = val
            retval.append(newvaldict)
        else:
            retval.extend(valdicts)

        sources.extend([(hxltag, date, datasetinfo['source'], datasetinfo['source_url']) for hxltag in hxltags])
    logger.info('Processed %s' % name)
    return retheaders, retval, sources


def get_tabular(configuration, level, downloader, scrapers=None, **kwargs):
    datasets = configuration['tabular_%s' % level]
    retheaders = [list(), list()]
    retval = list()
    sources = list()
    for name in datasets:
        if scrapers and not any(scraper in name for scraper in scrapers) and name != 'population':
            continue
        datasetinfo = datasets[name]
        format = datasetinfo['format']
        if format == 'json':
            iterator = read_json(downloader, datasetinfo, **kwargs)
            headers = None
        elif format == 'ole':
            headers, iterator = read_ole(downloader, datasetinfo, **kwargs)
        elif format in ['csv', 'xls', 'xlsx']:
            if 'dataset' in datasetinfo:
                headers, iterator = read_hdx(downloader, datasetinfo, **kwargs)
            else:
                headers, iterator = read_tabular(downloader, datasetinfo, **kwargs)
        else:
            raise ValueError('Invalid format %s for %s!' % (format, name))
        if 'source_url' not in datasetinfo:
            datasetinfo['source_url'] = datasetinfo['url']
        if 'date' not in datasetinfo or datasetinfo.get('force_date_today', False):
            datasetinfo['date'] = today_str
        _get_tabular(level, name, datasetinfo, headers, iterator, retheaders, retval, sources)
    return retheaders, retval, sources
