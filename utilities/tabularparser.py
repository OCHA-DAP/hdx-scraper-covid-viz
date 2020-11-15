# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from operator import itemgetter

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
    subsets = datasetinfo.get('subsets')
    if not subsets:
        subsets = [{'filter': datasetinfo.get('filter'), 'input_cols': datasetinfo.get('input_cols', list()),
                          'input_transforms': datasetinfo.get('input_transforms', dict()), 'process_cols':  datasetinfo.get('process_cols', list()),
                          'input_keep': datasetinfo.get('input_keep', list()), 'input_append': datasetinfo.get('input_append', list()),
                          'sum_cols': datasetinfo.get('sum_cols'), 'input_ignore_vals': datasetinfo.get('input_ignore_vals', list()),
                          'output_cols': datasetinfo.get('output_cols', list()), 'output_hxltags': datasetinfo.get('output_hxltags', list())}]
    use_hxl = datasetinfo.get('use_hxl', False)
    if use_hxl:
        hxlrow = next(iterator)
        while not hxlrow:
            hxlrow = next(iterator)
        exclude_tags = datasetinfo.get('exclude_tags', list())
        adm_cols = list()
        input_cols = list()
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
            input_cols.append(hxltag)
            columns.append(header)
        datasetinfo['adm_cols'] = adm_cols
        for subset in subsets:
            orig_input_cols = subset.get('input_cols', list())
            if not orig_input_cols:
                orig_input_cols.extend(input_cols)
            subset['input_cols'] = orig_input_cols
            orig_columns = subset.get('output_cols', list())
            if not orig_columns:
                orig_columns.extend(columns)
            subset['output_cols'] = orig_columns
            orig_hxltags = subset.get('output_hxltags', list())
            if not orig_hxltags:
                orig_hxltags.extend(input_cols)
            subset['output_hxltags'] = orig_hxltags
    else:
        hxlrow = None

    rowparser = RowParser(level, datasetinfo, headers, subsets)
    valuedicts = dict()
    for subset in subsets:
        for _ in subset['input_cols']:
            dict_of_lists_add(valuedicts, subset['filter'], dict())

    def add_row(row):
        adm, indicators_process = rowparser.do_set_value(row, name)
        if not adm:
            return
        for i, subset in enumerate(subsets):
            if not indicators_process[i]:
                continue
            filter = subset['filter']
            input_ignore_vals = subset.get('input_ignore_vals', list())
            input_transforms = subset.get('input_transforms', dict())
            sum_cols = subset.get('sum_cols')
            process_cols = subset.get('process_cols')
            input_append = subset.get('input_append', list())
            input_keep = subset.get('input_keep', list())
            for i, valcol in enumerate(subset['input_cols']):
                valuedict = valuedicts[filter][i]
                val = get_rowval(row, valcol)
                input_transform = input_transforms.get(valcol)
                if input_transform and val not in input_ignore_vals:
                    val = eval(input_transform.replace(valcol, 'val'))
                if sum_cols or process_cols:
                    dict_of_lists_add(valuedict, adm, val)
                else:
                    curval = valuedict.get(adm)
                    if valcol in input_append:
                        if curval:
                            val = curval + val
                    elif valcol in input_keep:
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

    for subset in subsets:
        output_cols = subset['output_cols']
        retheaders[0].extend(output_cols)
        output_hxltags = subset['output_hxltags']
        retheaders[1].extend(output_hxltags)
        valdicts = valuedicts[subset['filter']]
        process_cols = subset.get('process_cols')
        input_keep = subset.get('input_keep', list())
        sum_cols = subset.get('sum_cols')
        input_ignore_vals = subset.get('input_ignore_vals', list())
        valcols = subset['input_cols']
        # Indices of list sorted by length
        sorted_len_indices = sorted(range(len(valcols)), key=lambda k: len(valcols[k]), reverse=True)
        if process_cols:
            newvaldicts = [dict() for _ in process_cols]

            def text_replacement(string, adm):
                string = string.replace('#population', '#pzbgvjh')
                hasvalues = False
                for j in sorted_len_indices:
                    valcol = valcols[j]
                    if valcol not in string:
                        continue
                    if valcol in input_keep:
                        input_keep_index = 0
                    else:
                        input_keep_index = -1
                    val = valdicts[j][adm][input_keep_index]
                    if val is None or val == '' or val in input_ignore_vals:
                        val = 0
                    else:
                        hasvalues = True
                    string = string.replace(valcol, str(val))
                string = string.replace('#pzbgvjh', '#population')
                return string, hasvalues

            for i, process_col in enumerate(process_cols):
                valdict0 = valdicts[0]
                for adm in valdict0:
                    hasvalues = True
                    matches = regex.search(brackets, process_col, flags=regex.VERBOSE)
                    if matches:
                        for bracketed_str in matches.captures('rec'):
                            if any(bracketed_str in x for x in valcols):
                                continue
                            _, hasvalues_t = text_replacement(bracketed_str, adm)
                            if not hasvalues_t:
                                hasvalues = False
                                break
                    if hasvalues:
                        formula, hasvalues_t = text_replacement(process_col, adm)
                        if hasvalues_t:
                            formula = formula.replace('#population', 'population_lookup[adm]')
                            newvaldicts[i][adm] = eval(formula)
                        else:
                            newvaldicts[i][adm] = ''
                    else:
                        newvaldicts[i][adm] = ''
            retval.extend(newvaldicts)
        elif sum_cols:
            for sum_col in sum_cols:
                formula = sum_col['formula']
                mustbepopulated = sum_col.get('mustbepopulated', False)
                newvaldicts = [dict() for _ in valdicts]
                valdict0 = valdicts[0]
                for adm in valdict0:
                    for i, val in enumerate(valdict0[adm]):
                        if not val or val in input_ignore_vals:
                            exists = False
                        else:
                            exists = True
                            for valdict in valdicts[1:]:
                                val = valdict[adm][i]
                                if val is None or val == '' or val in input_ignore_vals:
                                    exists = False
                                    break
                        if mustbepopulated and not exists:
                            continue
                        for j, valdict in enumerate(valdicts):
                            newvaldicts[j][adm] = eval(f'newvaldicts[j].get(adm, 0.0) + {str(valdict[adm][i])}')
                formula = formula.replace('#population', '#pzbgvjh')
                for i in sorted_len_indices:
                    formula = formula.replace(valcols[i], 'newvaldicts[%d][adm]' % i)
                formula = formula.replace('#pzbgvjh', 'population_lookup[adm]')
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
        source = datasetinfo['source']
        if isinstance(source, str):
            source = {'default_source': source}
        source_url = datasetinfo['source_url']
        if isinstance(source_url, str):
            source_url = {'default_url': source_url}
        sources.extend([(hxltag, date, source.get(hxltag, source['default_source']), source_url.get(hxltag, source_url['default_url'])) for hxltag in output_hxltags])
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
                headers, iterator = read_hdx(downloader, datasetinfo)
            else:
                headers, iterator = read_tabular(downloader, datasetinfo, **kwargs)
        else:
            raise ValueError('Invalid format %s for %s!' % (format, name))
        if 'source_url' not in datasetinfo:
            datasetinfo['source_url'] = datasetinfo['url']
        if 'date' not in datasetinfo or datasetinfo.get('force_date_today', False):
            datasetinfo['date'] = today_str
        sort = datasetinfo.get('sort')
        if sort:
            keys = sort['keys']
            reverse = sort.get('reverse', False)
            iterator = sorted(list(iterator), key=itemgetter(*keys), reverse=reverse)
        _get_tabular(level, name, datasetinfo, headers, iterator, population_lookup, retheaders, retval, sources)
        if downloader != maindownloader:
            downloader.close()
        if population_lookup is not None:
            add_population(population_lookup, retheaders, retval)
    return retheaders, retval, sources
