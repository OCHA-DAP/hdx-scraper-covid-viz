# -*- coding: utf-8 -*-
from model import get_tabular_from_hdx
from model.rowparser import RowParser


def get_tabular(adms, datasetinfo, iterator, retheaders=[list(), list()], retval=list()):
    rowparser = RowParser(adms, datasetinfo)
    valcols = datasetinfo['val_cols']
    valuedicts = [dict() for _ in valcols]
    for row in iterator:
        adm = rowparser.do_set_value(row)
        if adm:
            for i, valcol in enumerate(valcols):
                valuedicts[i][adm] = row[valcol]
    retheaders[0].extend(datasetinfo['columns'])
    retheaders[1].extend(datasetinfo['hxltags'])
    retval.extend(valuedicts)
    return retheaders, retval


def get_tabular_hdx(configuration, adms, national_subnational, downloader):
    datasets = configuration['tabular_%s' % national_subnational]
    retheaders = [list(), list()]
    retval = list()
    for name in datasets:
        datasetinfo = datasets[name]
        headers, iterator = get_tabular_from_hdx(downloader, datasetinfo)
        get_tabular(adms, datasetinfo, iterator, retheaders, retval)
    return retheaders, retval



