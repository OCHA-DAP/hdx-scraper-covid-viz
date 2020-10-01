# -*- coding: utf-8 -*-
import logging
from datetime import datetime

import regex

from hdx.utilities.dateparse import parse_date, get_datetime_from_timestamp
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import Download
from hdx.utilities.text import number_format, get_fraction_str, get_numeric_if_possible

from model import today_str, add_population
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


def _get_tabular(level, name, datasetinfo, headers, iterator, population_lookup, retheaders=[list(), list()], retval=list(), sources=list()):
    indicatorcols = datasetinfo.get('indicator_cols')
    if not indicatorcols:
        indicatorcols = [{'filter_col': datasetinfo.get('filter_col'), 'val_cols': datasetinfo.get('val_cols', list()),
                          'val_fns': datasetinfo.get('val_fns', dict()), 'eval_cols':  datasetinfo.get('eval_cols', list()),
                          'keep_cols': datasetinfo.get('keep_cols', list()), 'append_cols': datasetinfo.get('append_cols', list()),
                          'total_cols': datasetinfo.get('total_cols'), 'ignore_vals': datasetinfo.get('ignore_vals', list()),
                          'columns': datasetinfo.get('columns', list()), 'hxltags': datasetinfo.get('hxltags', list())}]
    use_hxl = datasetinfo.get('use_hxl', False)
    if use_hxl:
        hxlrow = next(iterator)
        while not hxlrow:
            hxlrow = next(iterator)
        exclude_tags = datasetinfo.get('exclude_tags', list())
        adm_cols = list()
        val_cols = list()
        columns = list()
        for header in headers:
            hxltag = hxlrow[header]
            if not hxltag or hxltag in exclude_tags:
                continue
            if '#country' in hxltag:
                if 'code' in hxltag:
                    if len(adm_cols) == 0:
                        adm_cols.append(hxltag)
                    else:
                        adm_cols[0] = hxltag
                continue
            if '#adm1' in hxltag:
                if 'code' in hxltag:
                    if len(adm_cols) == 0:
                        adm_cols.append(None)
                    if len(adm_cols) == 1:
                        adm_cols.append(hxltag)
                continue
            if hxltag == datasetinfo.get('date_col') and datasetinfo.get('include_date', False) is False:
                continue
            val_cols.append(hxltag)
            columns.append(header)
        datasetinfo['adm_cols'] = adm_cols
        for indicatorcol in indicatorcols:
            orig_val_cols = indicatorcol.get('val_cols', list())
            if not orig_val_cols:
                orig_val_cols.extend(val_cols)
            indicatorcol['val_cols'] = orig_val_cols
            orig_columns = indicatorcol.get('columns', list())
            if not orig_columns:
                orig_columns.extend(columns)
            indicatorcol['columns'] = orig_columns
            orig_hxltags = indicatorcol.get('hxltags', list())
            if not orig_hxltags:
                orig_hxltags.extend(val_cols)
            indicatorcol['hxltags'] = orig_hxltags
    else:
        hxlrow = None

    rowparser = RowParser(level, datasetinfo, headers)
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
                filtercols = filtercol.split('|')
                match = True
                for filterstr in filtercols:
                    filter = filterstr.split('=')
                    if row[filter[0]] != filter[1]:
                        match = False
                        break
                if not match:
                    continue
            total_cols = indicatorcol.get('total_cols')
            eval_cols = indicatorcol.get('eval_cols')
            append_cols = indicatorcol.get('append_cols', list())
            keep_cols = indicatorcol.get('keep_cols', list())
            for i, valcol in enumerate(indicatorcol['val_cols']):
                valuedict = valuedicts[filtercol][i]
                val = get_rowval(row, valcol)
                if total_cols or eval_cols:
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
        if hxlrow:
            newrow = dict()
            for header in row:
                newrow[hxlrow[header]] = row[header]
            row = newrow
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
        keep_cols = indicatorcol.get('keep_cols', list())
        total_cols = indicatorcol.get('total_cols')
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
                        formula, hasvalues_t = text_replacement(eval_col, adm)
                        if hasvalues_t:
                            formula = formula.replace('#population', 'population_lookup[adm]')
                            newvaldicts[i][adm] = eval(formula)
                        else:
                            newvaldicts[i][adm] = ''
                    else:
                        newvaldicts[i][adm] = ''
            retval.extend(newvaldicts)
        elif total_cols:
            for total_col in total_cols:
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
                            newvaldicts[j][adm] = newvaldicts[j].get(adm, 0.0) + eval(val_fn.replace(valcol, 'get_numeric_if_possible(valdict[adm][i])'))
                for i, valcol in enumerate(valcols):
                    formula = formula.replace(valcol, 'newvaldicts[%d][adm]' % i)
                formula = formula.replace('#population', 'population_lookup[adm]')
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


def get_tabular(basic_auths, configuration, level, maindownloader, scrapers=None, population_lookup=None, **kwargs):
    datasets = configuration['tabular_%s' % level]
    retheaders = [list(), list()]
    retval = list()
    sources = list()
    for name in datasets:
        if scrapers:
            if not any(scraper in name for scraper in scrapers):
                continue
        else:
            if name == 'population':
                continue
        logger.info('Processing %s' % name)
        basic_auth = basic_auths.get(name)
        if basic_auth is None:
            downloader = maindownloader
        else:
            downloader = Download(basic_auth=basic_auth, rate_limit={'calls': 1, 'period': 0.1})
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
        _get_tabular(level, name, datasetinfo, headers, iterator, population_lookup, retheaders, retval, sources)
        if downloader != maindownloader:
            downloader.close()
        if population_lookup is not None:
            add_population(population_lookup, retheaders, retval)
    return retheaders, retval, sources
