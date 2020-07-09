# -*- coding: utf-8 -*-
from hdx.utilities.dictandlist import dict_of_lists_add

from model.readers import read_json, read_ole, read_hdx, read_tabular


def add_additional_json(configuration, downloader, json):
    for datasetinfo in configuration.get('additional_json', list()):
        name = datasetinfo['name']
        format = datasetinfo['format']
        if format == 'json':
            iterator = read_json(downloader, datasetinfo)
            headers = None
        elif format == 'ole':
            headers, iterator = read_ole(downloader, datasetinfo)
        elif format in ['csv', 'xls', 'xlsx']:
            if 'dataset' in datasetinfo:
                headers, iterator = read_hdx(downloader, datasetinfo)
            else:
                headers, iterator = read_tabular(downloader, datasetinfo)
        else:
            raise ValueError('Invalid format %s for %s!' % (format, name))
        hxlrow = next(iterator)
        for row in iterator:
            newrow = dict()
            for key in row:
                hxltag = hxlrow[key]
                if hxltag != '':
                    newrow[hxlrow[key]] = row[key]
            dict_of_lists_add(json, '%s_data' % name, newrow)