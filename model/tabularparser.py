# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from os.path import join

from hdx.data.dataset import Dataset
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.path import temp_dir
from jsonpath_ng import parse
from olefile import olefile

from model import today, today_str, get_percent, get_date_from_dataset_date, template
from model.rowparser import RowParser

logger = logging.getLogger(__name__)


def _get_tabular(adms, name, datasetinfo, headers, iterator, retheaders=[list(), list()], retval=list(), sources=list()):
    rowparser = RowParser(adms, datasetinfo, headers)
    indicatorcols = datasetinfo.get('indicator_cols')
    if not indicatorcols:
        indicatorcols = [{'filter_col': datasetinfo.get('filter_col'), 'val_cols': datasetinfo['val_cols'], 'total_col': datasetinfo.get('total_col'),
                          'ignore_vals': datasetinfo.get('ignore_vals', list()), 'add_fns': datasetinfo.get('add_fns'),
                          'columns': datasetinfo['columns'], 'hxltags': datasetinfo['hxltags']}]
    valuedicts = dict()
    for indicatorcol in indicatorcols:
        for _ in indicatorcol['val_cols']:
            dict_of_lists_add(valuedicts, indicatorcol['filter_col'], dict())

    def add_row(row):
        adm, _ = rowparser.do_set_value(row)
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
            totalcol = indicatorcol.get('total_col')
            for i, valcol in enumerate(indicatorcol['val_cols']):
                valuedict = valuedicts[filtercol][i]
                val = row[valcol]
                if totalcol:
                    dict_of_lists_add(valuedict, adm, val)
                else:
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
        ignore_vals = indicatorcol.get('ignore_vals', list())
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


def get_url(url, **kwargs):
    for kwarg in kwargs:
        exec('%s=%s' % (kwarg, kwargs[kwarg]))
    match = template.search(url)
    if match:
        template_string = match.group()
        replace_string = eval(template_string[2:-2])
        url = url.replace(template_string, replace_string)
    return url


def get_tabular_source(downloader, datasetinfo, **kwargs):
    url = get_url(datasetinfo['url'], **kwargs)
    sheetname = datasetinfo.get('sheetname')
    headers = datasetinfo['headers']
    if isinstance(headers, list):
        kwargs['fill_merged_cells'] = True
    format = datasetinfo['format']
    return downloader.get_tabular_rows(url, sheet=sheetname, headers=headers, dict_form=True, format=format, **kwargs)


def get_ole_source(downloader, datasetinfo, **kwargs):
    url = get_url(datasetinfo['url'], **kwargs)
    with temp_dir('ole') as folder:
        path = downloader.download_file(url, folder, 'olefile')
        ole = olefile.OleFileIO(path)
        data = ole.openstream('Workbook').getvalue()
        outputfile = join(folder, 'excel_file.xls')
        with open(outputfile, 'wb') as f:
            f.write(data)
        datasetinfo['url'] = outputfile
        datasetinfo['format'] = 'xls'
        return get_tabular_source(downloader, datasetinfo, **kwargs)


def get_json_source(downloader, datasetinfo, **kwargs):
    url = get_url(datasetinfo['url'], **kwargs)
    response = downloader.download(url)
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
    datasetinfo['date'] = get_date_from_dataset_date(dataset)
    if 'source' not in datasetinfo:
        datasetinfo['source'] = dataset['dataset_source']
    if 'source_url' not in datasetinfo:
        datasetinfo['source_url'] = dataset.get_hdx_url()
    return get_tabular_source(downloader, datasetinfo)


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
            iterator = get_json_source(downloader, datasetinfo, adms=adms)
            headers = None
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
        _get_tabular(adms, name, datasetinfo, headers, iterator, retheaders, retval, sources)
    return retheaders, retval, sources


