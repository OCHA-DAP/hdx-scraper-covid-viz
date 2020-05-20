# -*- coding: utf-8 -*-
import logging
from datetime import datetime

from hdx.data.dataset import Dataset
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from jsonpath_ng import parse

from model import today, logger, today_str
from model.rowparser import RowParser

logger = logging.getLogger(__name__)


def _get_tabular(adms, name, datasetinfo, iterator, retheaders=[list(), list()], retval=list(), sources=list()):
    rowparser = RowParser(adms, datasetinfo)
    indicatorcols = datasetinfo.get('indicator_cols')
    if not indicatorcols:
        indicatorcols = [{'filter_col': None, 'val_cols': datasetinfo['val_cols'], 'total_col': datasetinfo.get('total_col'),
                          'ignore_vals': datasetinfo.get('ignore_vals'), 'add_fns': datasetinfo.get('add_fns'),
                          'columns': datasetinfo['columns'], 'hxltags': datasetinfo['hxltags']}]
    valuedicts = dict()
    for indicatorcol in indicatorcols:
        for _ in indicatorcol['val_cols']:
            dict_of_lists_add(valuedicts, indicatorcol['filter_col'], dict())
    for row in iterator:
        if not isinstance(row, dict):
            row = row.value
        adm = rowparser.do_set_value(row)
        if not adm:
            continue
        for indicatorcol in indicatorcols:
            filtercol = indicatorcol['filter_col']
            if filtercol:
                filter = filtercol.split('=')
                if row[filter[0]] != filter[1]:
                    continue
            totalcol = indicatorcol.get('total_col')
            for i, valcol in enumerate(indicatorcol['val_cols']):
                valuedict = valuedicts[filtercol][i]
                val = row[valcol]
                if totalcol:
                    dict_of_lists_add(valuedict, adm, val)
                else:
                    valuedict[adm] = val
    date = datasetinfo.get('modified')
    if date:
        date = parse_date(date)
    else:
        date = rowparser.maxdate
        if date == 0:
            raise ValueError('No date given in datasetinfo or as a column!')
        if rowparser.datetype == 'date':
            date = parse_date(date)
        elif rowparser.datetype == 'int':
            if date > today.timestamp():
                date = date / 1000
            date = datetime.fromtimestamp(date)
        else:
            raise ValueError('No date type specified!')
    date = date.strftime('%Y-%m-%d')
    for indicatorcol in indicatorcols:
        retheaders[0].extend(indicatorcol['columns'])
        hxltags = indicatorcol['hxltags']
        retheaders[1].extend(hxltags)
        valdicts = valuedicts[indicatorcol['filter_col']]
        total_col = indicatorcol.get('total_col')
        ignore_vals = indicatorcol.get('ignore_vals')
        add_fns = indicatorcol.get('add_fns')
        valcols = indicatorcol['val_cols']
        if total_col:
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
                        newvaldicts[j][adm] = newvaldicts[j].get(adm, 0.0) + eval(add_fns[j].replace(valcols[j], 'valdict[adm][i]'))
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


def get_tabular_source(downloader, datasetinfo, **kwargs):
    url = datasetinfo['url']
    sheetname = datasetinfo.get('sheetname')
    headers = datasetinfo['headers']
    format = datasetinfo['format']
    headers, iterator = downloader.get_tabular_rows(url, sheet=sheetname, headers=headers, dict_form=True, format=format, **kwargs)
    return iterator


def get_json_source(downloader, datasetinfo):
    response = downloader.download(datasetinfo['url'])
    json = response.json()
    expression = datasetinfo.get('jsonpath')
    if expression:
        expression = parse(expression)
        return expression.find(json)
    return json


def get_hdx_source(downloader, datasetinfo):
    dataset_name = datasetinfo['dataset']
    dataset = Dataset.read_from_hdx(dataset_name)
    format = datasetinfo['format']
    url = None
    for resource in dataset.get_resources():
        if resource['format'] == format.upper():
            url = resource['url']
            break
    if not url:
        logger.error('Cannot find %s resource in %s!' % (format, dataset_name))
        return None, None
    datasetinfo['url'] = url
    datasetinfo['modified'] = dataset['last_modified']
    if 'source' not in datasetinfo:
        datasetinfo['source'] = dataset['dataset_source']
    if 'source_url' not in datasetinfo:
        datasetinfo['source_url'] = dataset.get_hdx_url()
    return get_tabular_source(downloader, datasetinfo)


def get_tabular(configuration, adms, national_subnational, downloader):
    datasets = configuration['tabular_%s' % national_subnational]
    retheaders = [list(), list()]
    retval = list()
    sources = list()
    for name in datasets:
        datasetinfo = datasets[name]
        format = datasetinfo['format']
        if format == 'json':
            iterator = get_json_source(downloader, datasetinfo)
        elif format in ['csv', 'xls', 'xlsx']:
            if 'dataset' in datasetinfo:
                iterator = get_hdx_source(downloader, datasetinfo)
            else:
                iterator = get_tabular_source(downloader, datasetinfo)
        else:
            raise ValueError('Invalid format %s for %s!' % (format, name))
        if 'source_url' not in datasetinfo:
            datasetinfo['source_url'] = datasetinfo['url']
        if 'modified' not in datasetinfo:
            datasetinfo['modified'] = today_str
        _get_tabular(adms, name, datasetinfo, iterator, retheaders, retval, sources)
    return retheaders, retval, sources


