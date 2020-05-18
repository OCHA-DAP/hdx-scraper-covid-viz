# -*- coding: utf-8 -*-
import logging
from datetime import datetime

from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from jsonpath_ng import parse

from model import get_tabular_from_hdx, today
from model.rowparser import RowParser

logger = logging.getLogger(__name__)


def get_tabular(adms, name, datasetinfo, iterator, retheaders=[list(), list()], retval=list(), sources=list()):
    rowparser = RowParser(adms, datasetinfo)
    indicatorcols = datasetinfo.get('indicator_cols')
    if not indicatorcols:
        indicatorcols = [{'filter_col': None, 'val_cols': datasetinfo['val_cols'], 'columns': datasetinfo['columns'],
                          'hxltags': datasetinfo['hxltags']}]
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
            for i, valcol in enumerate(indicatorcol['val_cols']):
                valuedicts[filtercol][i][adm] = row[valcol]
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
        retval.extend(valuedicts[indicatorcol['filter_col']])
        sources.extend([[hxltag, date, datasetinfo['source']] for hxltag in hxltags])
    logger.info('Processed %s' % name)
    return retheaders, retval, sources


def get_tabular_json(configuration, adms, downloader, national_subnational):
    datasets = configuration['tabular_json_%s' % national_subnational]
    retheaders = [list(), list()]
    retval = list()
    sources = list()
    for name in datasets:
        datasetinfo = datasets[name]
        url = datasetinfo['url']
        response = downloader.download(url)
        json = response.json()
        expression = datasetinfo.get('jsonpath')
        if expression:
            expression = parse(expression)
            iterator = expression.find(json)
        else:
            iterator = json
        get_tabular(adms, name, datasetinfo, iterator, retheaders, retval, sources)
    return retheaders, retval, sources


def get_tabular_hdx(configuration, adms, national_subnational, downloader):
    datasets = configuration['tabular_hdx_%s' % national_subnational]
    retheaders = [list(), list()]
    retval = list()
    sources = list()
    for name in datasets:
        datasetinfo = datasets[name]
        dataset, headers, iterator = get_tabular_from_hdx(downloader, datasetinfo)
        datasetinfo['modified'] = dataset['last_modified']
        datasetinfo['source'] = dataset.get_hdx_url()
        get_tabular(adms, name, datasetinfo, iterator, retheaders, retval, sources)
    return retheaders, retval, sources



