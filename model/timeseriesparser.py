# -*- coding: utf-8 -*-
import copy
import logging
from datetime import datetime

from hdx.utilities.dateparse import parse_date
from hdx.location.country import Country


from model import today, today_str, get_percent
from model.rowparser import RowParser
from model.tabularparser import get_json_source, get_hdx_source, get_tabular_source, get_ole_source

logger = logging.getLogger(__name__)


def _get_timeseries(timeseries, adms, datasetinfo, headers, iterator, sources=list()):
    rowparser = RowParser(adms, datasetinfo, headers, maxdateonly=False)
    valcol = datasetinfo['val_col']
    ignore_vals = datasetinfo.get('ignore_vals', list())
    indicatorcols = datasetinfo.get('indicator_cols')
    if not indicatorcols:
        indicatorcols = [{'filter_col': datasetinfo.get('filter_col'), 'name': datasetinfo['name']}]

    def add_row(row):
        adm, date = rowparser.do_set_value(row)
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
            val = row[valcol]
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
            if date > today.timestamp():
                date = date / 1000
            date = datetime.fromtimestamp(date)
        else:
            raise ValueError('No date type specified!')
    date = date.strftime('%Y-%m-%d')
    for indicatorcol in indicatorcols:
        sources.append([indicatorcol['name'], date, datasetinfo['source'], datasetinfo['source_url']])
    logger.info('Processed %s' % datasetinfo['name'])
    return sources


def get_timeseries(timeseries, configuration, adms, national_subnational, downloader, scraper=None):
    datasets = configuration['timeseries_%s' % national_subnational]
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
                iterator = get_json_source(downloader, datasetinfo, adms=adms)
            elif format == 'ole':
                headers, iterator = get_ole_source(downloader, datasetinfo, adms=adms)
            elif format in ['csv', 'xls', 'xlsx']:
                if 'dataset' in datasetinfo:
                    headers, iterator = get_hdx_source(downloader, datasetinfo)
                else:
                    headers, iterator = get_tabular_source(downloader, datasetinfo, adms=adms)
            else:
                raise ValueError('Invalid format %s for %s!' % (format, name))
            if 'source_url' not in datasetinfo:
                datasetinfo['source_url'] = datasetinfo['url']
            if 'date' not in datasetinfo:
                datasetinfo['date'] = today_str
            _get_timeseries(timeseries, adms, datasetinfo, headers, iterator, sources)
    return sources


