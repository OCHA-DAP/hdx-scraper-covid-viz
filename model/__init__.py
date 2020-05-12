import logging

from hdx.data.dataset import Dataset

logger = logging.getLogger(__name__)


def get_tabular_from_hdx(downloader, datasetinfo):
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
    sheetname = datasetinfo.get('sheetname')
    headers = datasetinfo['headers']
    return downloader.get_tabular_rows(url, sheet=sheetname, headers=headers, dict_form=True, format=format)
