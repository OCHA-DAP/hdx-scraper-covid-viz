# -*- coding: utf-8 -*-
from model import get_tabular_from_hdx
from model.rowparser import RowParser


def get_tabular_hdx(configuration, countryiso3s, downloader):
    datasets = configuration['tabular_hdx']
    retheaders = [list(), list()]
    retval = list()
    for name in datasets:
        datasetinfo = datasets[name]
        headers, iterator = get_tabular_from_hdx(downloader, datasetinfo)
        valuedict = dict()
        valcol = datasetinfo['val_col']
        rowparser = RowParser(countryiso3s, datasetinfo)
        for row in iterator:
            countryiso = rowparser.do_set_value(row)
            if countryiso:
                valuedict[countryiso] = row[valcol]
        retheaders[0].append(datasetinfo['header'])
        retheaders[1].append(datasetinfo['hxltag'])
        retval.append(valuedict)
    return retheaders, retval



