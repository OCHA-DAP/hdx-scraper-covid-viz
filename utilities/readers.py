# -*- coding: utf-8 -*-
import logging
from os.path import join

from hdx.data.dataset import Dataset
from hdx.utilities.path import temp_dir
from jsonpath_ng import parse
from olefile import olefile

from model import template
from utilities import get_date_from_dataset_date

logger = logging.getLogger(__name__)


def get_url(url, **kwargs):
    for kwarg in kwargs:
        exec('%s=%s' % (kwarg, kwargs[kwarg]))
    match = template.search(url)
    if match:
        template_string = match.group()
        replace_string = eval(template_string[2:-2])
        url = url.replace(template_string, replace_string)
    return url


def read_tabular(downloader, datasetinfo, **kwargs):
    url = get_url(datasetinfo['url'], **kwargs)
    sheet = datasetinfo.get('sheet')
    headers = datasetinfo['headers']
    if isinstance(headers, list):
        kwargs['fill_merged_cells'] = True
    format = datasetinfo['format']
    compression = datasetinfo.get('compression')
    if compression:
        kwargs['compression'] = compression
    return downloader.get_tabular_rows(url, sheet=sheet, headers=headers, dict_form=True, format=format, **kwargs)


def read_ole(downloader, datasetinfo, **kwargs):
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
        return read_tabular(downloader, datasetinfo, **kwargs)


def read_json(downloader, datasetinfo, **kwargs):
    url = get_url(datasetinfo['url'], **kwargs)
    response = downloader.download(url)
    json = response.json()
    expression = datasetinfo.get('jsonpath')
    if expression:
        expression = parse(expression)
        return expression.find(json)
    return json


def read_hdx_metadata(datasetinfo):
    dataset_name = datasetinfo['dataset']
    dataset = Dataset.read_from_hdx(dataset_name)
    format = datasetinfo['format']
    url = datasetinfo.get('url')
    if not url:
        for resource in dataset.get_resources():
            if resource['format'] == format.upper():
                url = resource['url']
                break
        if not url:
            logger.error('Cannot find %s resource in %s!' % (format, dataset_name))
            return None, None
        datasetinfo['url'] = url
    if 'date' not in datasetinfo:
        datasetinfo['date'] = get_date_from_dataset_date(dataset)
    if 'source' not in datasetinfo:
        datasetinfo['source'] = dataset['dataset_source']
    if 'source_url' not in datasetinfo:
        datasetinfo['source_url'] = dataset.get_hdx_url()


def read_hdx(downloader, datasetinfo):
    read_hdx_metadata(datasetinfo)
    return read_tabular(downloader, datasetinfo)