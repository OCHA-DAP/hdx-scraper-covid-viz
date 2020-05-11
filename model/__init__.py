from hdx.data.dataset import Dataset


def get_tabular_from_hdx(downloader, datasetinfo):
    dataset = Dataset.read_from_hdx(datasetinfo['dataset'])
    url = dataset.get_resource()['url']
    sheetname = datasetinfo['sheetname']
    headers = datasetinfo['headers']
    format = datasetinfo['format']
    return downloader.get_tabular_rows(url, sheet=sheetname, headers=headers, dict_form=True, format=format)
