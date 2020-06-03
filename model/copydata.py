# -*- coding: utf-8 -*-
import logging

from model import today_str
from model.rowparser import RowParser
from model.readers import read_tabular, read_ole, read_hdx

logger = logging.getLogger(__name__)


def _get_copy(adms, name, datasetinfo, headers, iterator, retheaders=[list(), list()], retval=list(), sources=list()):
    valuedicts = list()
    val_cols = list()
    hxltags = list()
    hxlrow = next(iterator)
    while not hxlrow:
        hxlrow = next(iterator)
    adm_cols = list()
    for header in headers:
        hxltag = hxlrow[header]
        if '#country' in hxltag:
            if 'code' in hxltag:
                if len(adm_cols) == 0:
                    adm_cols.append(header)
                else:
                    adm_cols[0] = header
            continue
        if '#adm1' in hxltag:
            if 'code' in hxltag:
                if len(adm_cols) == 0:
                    adm_cols.append(None)
                if len(adm_cols) == 1:
                    adm_cols.append(header)
            continue
        val_cols.append(header)
        hxltags.append(hxltag)
        valuedicts.append(dict())
    rowparser = RowParser(adms, {'adm_cols': adm_cols}, headers)
    for row in iterator:
        adm, _ = rowparser.do_set_value(row, name)
        if adm:
            for i, val_col in enumerate(val_cols):
                valuedicts[i][adm] = row[val_col]
    retheaders[0].extend(val_cols)
    retheaders[1].extend(hxltags)
    retval.extend(valuedicts)
    date = datasetinfo.get('date')
    sources.extend([[hxltag, date, datasetinfo['source'], datasetinfo['source_url']] for hxltag in hxltags])
    logger.info('Processed %s' % name)
    return retheaders, retval, sources


def get_copy(configuration, adms, national_subnational, downloader, scraper=None):
    datasets = configuration['copy_%s' % national_subnational]
    retheaders = [list(), list()]
    retval = list()
    sources = list()
    for name in datasets:
        if scraper and scraper not in name:
            continue
        datasetinfo = datasets[name]
        format = datasetinfo['format']
        if format == 'ole':
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
        _get_copy(adms, name, datasetinfo, headers, iterator, retheaders, retval, sources)
    return retheaders, retval, sources


