# -*- coding: utf-8 -*-
import copy
import logging
from datetime import datetime

from hdx.utilities.dateparse import parse_date, get_datetime_from_timestamp
from hdx.location.country import Country
from hdx.utilities.text import number_format, get_fraction_str, get_numeric_if_possible


from model import today, today_str, get_rowval
from model.rowparser import RowParser
from model.readers import read_tabular, read_ole, read_json, read_hdx

logger = logging.getLogger(__name__)


def _get_timeseries(timeseries, level, datasetinfo, headers, iterator, sources=list()):
    rowparser = RowParser(level, datasetinfo, headers, maxdateonly=False)
    name = datasetinfo['name']
    valcol = datasetinfo['val_col']
    ignore_vals = datasetinfo.get('ignore_vals', list())
    indicatorcols = datasetinfo.get('indicator_cols')
    if not indicatorcols:
        indicatorcols = [{'filter_col': datasetinfo.get('filter_col'), 'name': name}]

    def add_row(row):
        adm, date = rowparser.do_set_value(row, name)
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
            val = get_rowval(row, valcol)
            if val in ignore_vals:
                continue
            timeseries.append([adm, date, indicatorcol['name'], val])

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
            date = get_datetime_from_timestamp(date)
        else:
            raise ValueError('No date type specified!')
    date = date.strftime('%Y-%m-%d')
    for indicatorcol in indicatorcols:
        sources.append((indicatorcol['name'], date, datasetinfo['source'], datasetinfo['source_url']))
    logger.info('Processed %s' % name)
    return sources


def get_timeseries(timeseries, configuration, level, downloader, scraper=None, **kwargs):
    datasets = configuration['timeseries_%s' % level]
    sources = list()
    for name in datasets:
        if scraper and scraper not in name:
            continue
        origdatasetinfo = datasets[name]
        indicators = origdatasetinfo.get('indicators')
        datasetinfos = list()
        if indicators:
            for indicator in indicators:
                datasetinfo = copy.deepcopy(origdatasetinfo)
                datasetinfo['url'] = datasetinfo['url'] % indicator['url']
                datasetinfo['name'] = indicator['name']
                datasetinfos.append(datasetinfo)
        else:
            datasetinfos = [origdatasetinfo]
        for datasetinfo in datasetinfos:
            format = datasetinfo['format']
            if format == 'json':
                headers = None
                iterator = read_json(downloader, datasetinfo, **kwargs)
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
            _get_timeseries(timeseries, level, datasetinfo, headers, iterator, sources)
    return sources


