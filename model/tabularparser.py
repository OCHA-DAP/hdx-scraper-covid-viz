# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from hdx.location.country import Country

from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add

from model import today, today_str, get_percent, get_rowval, get_date_from_timestamp
from model.rowparser import RowParser
from model.readers import read_tabular, read_ole, read_json, read_hdx

logger = logging.getLogger(__name__)


def _get_tabular(adms, name, datasetinfo, headers, iterator, retheaders=[list(), list()], retval=list(), sources=list()):
    rowparser = RowParser(adms, datasetinfo, headers)
    indicatorcols = datasetinfo.get('indicator_cols')
    if not indicatorcols:
        indicatorcols = [{'filter_col': datasetinfo.get('filter_col'), 'val_cols': datasetinfo['val_cols'], 'val_fns': datasetinfo.get('val_fns'),
                          'eval_cols':  datasetinfo.get('eval_cols', list()), 'append_cols': datasetinfo.get('append_cols', list()),
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
            for i, valcol in enumerate(indicatorcol['val_cols']):
                valuedict = valuedicts[filtercol][i]
                val = get_rowval(row, valcol)
                if total_col or eval_cols:
                    dict_of_lists_add(valuedict, adm, val)
                else:
                    if valcol in append_cols:
                        curval = valuedict.get(adm)
                        if curval:
                            val = curval + val
                    valuedict[adm] = val

    for row in iterator:
        if not isinstance(row, dict):
            row = row.value
        for newrow in rowparser.flatten(row):
            add_row(newrow)

    date = datasetinfo.get('date')
    if date:
        date = parse_date(date)
    else:
        date = rowparser.get_maxdate()
        if date == 0:
            raise ValueError('No date given in datasetinfo or as a column!')
        if rowparser.datetype == 'date':
            date = parse_date(date)
        elif rowparser.datetype == 'int':
            date = get_date_from_timestamp(date)
        else:
            raise ValueError('No date type specified!')
    date = date.strftime('%Y-%m-%d')
    for indicatorcol in indicatorcols:
        retheaders[0].extend(indicatorcol['columns'])
        hxltags = indicatorcol['hxltags']
        retheaders[1].extend(hxltags)
        valdicts = valuedicts[indicatorcol['filter_col']]
        eval_cols = indicatorcol.get('eval_cols')
        total_col = indicatorcol.get('total_col')
        ignore_vals = indicatorcol.get('ignore_vals', list())
        val_fns = indicatorcol.get('val_fns')
        valcols = indicatorcol['val_cols']
        if eval_cols:
            newvaldicts = [dict() for _ in eval_cols]
            for i, eval_col in enumerate(eval_cols):
                valdict0 = valdicts[0]
                for adm in valdict0:
                    newvaldicts[i][adm] = eval_col
                    hasvalues = False
                    for j, valcol in enumerate(valcols):
                        val = valdicts[j][adm][-1]
                        if not val or val in ignore_vals:
                            val = 0
                        else:
                            val = eval(val_fns[j].replace(valcol, val))
                            hasvalues = True
                        newvaldicts[i][adm] = newvaldicts[i][adm].replace(valcol, str(val))
                    if hasvalues:
                        newvaldicts[i][adm] = eval(newvaldicts[i][adm])
                    else:
                        newvaldicts[i][adm] = ''
            retval.extend(newvaldicts)
        elif total_col:
            newvaldicts = [dict() for _ in valdicts]
            valdict0 = valdicts[0]
            for adm in valdict0:
                for i, val in enumerate(valdict0[adm]):
                    if not val or val in ignore_vals:
                        continue
                    exists = True
                    for valdict in valdicts[1:]:
                        val = valdict[adm]
                        if not val or val in ignore_vals:
                            exists = False
                            break
                    if not exists:
                        continue
                    for j, valdict in enumerate(valdicts):
                        newvaldicts[j][adm] = newvaldicts[j].get(adm, 0.0) + eval(val_fns[j].replace(valcols[j], 'valdict[adm][i]'))
            for i, valcol in enumerate(valcols):
                total_col = total_col.replace(valcol, 'newvaldicts[%d][adm]' % i)
            newvaldict = dict()
            for adm in valdicts[0].keys():
                try:
                    val = eval(total_col)
                except (ValueError, TypeError, KeyError):
                    val = ''
                newvaldict[adm] = val
            retval.append(newvaldict)
        else:
            retval.extend(valdicts)

        sources.extend([[hxltag, date, datasetinfo['source'], datasetinfo['source_url']] for hxltag in hxltags])
    logger.info('Processed %s' % name)
    return retheaders, retval, sources


def get_tabular(configuration, adms, national_subnational, downloader, scraper=None):
    datasets = configuration['tabular_%s' % national_subnational]
    retheaders = [list(), list()]
    retval = list()
    sources = list()
    for name in datasets:
        if scraper and scraper not in name:
            continue
        datasetinfo = datasets[name]
        format = datasetinfo['format']
        if format == 'json':
            iterator = read_json(downloader, datasetinfo, adms=adms)
            headers = None
        elif format == 'ole':
            headers, iterator = read_ole(downloader, datasetinfo, adms=adms)
        elif format in ['csv', 'xls', 'xlsx']:
            if 'dataset' in datasetinfo:
                headers, iterator = read_hdx(downloader, datasetinfo)
            else:
                headers, iterator = read_tabular(downloader, datasetinfo, adms=adms)
        else:
            raise ValueError('Invalid format %s for %s!' % (format, name))
        if 'source_url' not in datasetinfo:
            datasetinfo['source_url'] = datasetinfo['url']
        if 'date' not in datasetinfo:
            datasetinfo['date'] = today_str
        datasetinfo['adm_mappings'] = configuration['adm_mappings']
        _get_tabular(adms, name, datasetinfo, headers, iterator, retheaders, retval, sources)
    return retheaders, retval, sources


