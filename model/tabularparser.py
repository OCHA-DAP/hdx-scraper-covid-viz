# -*- coding: utf-8 -*-
import logging

from hdx.utilities.dictandlist import dict_of_lists_add
from jsonpath_ng import parse

from model import get_tabular_from_hdx
from model.rowparser import RowParser

logger = logging.getLogger(__name__)


def get_tabular(adms, name, datasetinfo, iterator, retheaders=[list(), list()], retval=list()):
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
    for indicatorcol in indicatorcols:
        retheaders[0].extend(indicatorcol['columns'])
        retheaders[1].extend(indicatorcol['hxltags'])
        retval.extend(valuedicts[indicatorcol['filter_col']])
    logger.info('Processed %s' % name)
    return retheaders, retval


def get_tabular_json(configuration, adms, downloader, national_subnational):
    datasets = configuration['tabular_json_%s' % national_subnational]
    retheaders = [list(), list()]
    retval = list()
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
        get_tabular(adms, name, datasetinfo, iterator, retheaders, retval)
    return retheaders, retval


def get_tabular_hdx(configuration, adms, national_subnational, downloader):
    datasets = configuration['tabular_hdx_%s' % national_subnational]
    retheaders = [list(), list()]
    retval = list()
    for name in datasets:
        datasetinfo = datasets[name]
        headers, iterator = get_tabular_from_hdx(downloader, datasetinfo)
        get_tabular(adms, name, datasetinfo, iterator, retheaders, retval)
    return retheaders, retval



