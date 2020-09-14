# -*- coding: utf-8 -*-
import logging
from os.path import join

from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.saver import save_json

from utilities.readers import read_json, read_ole, read_hdx, read_tabular


logger = logging.getLogger(__name__)


class jsonoutput:
    def __init__(self, configuration, updatetabs):
        self.json_configuration = configuration['json']
        self.updatetabs = updatetabs
        self.json = dict()

    def add_data_row(self, name, row):
        dict_of_lists_add(self.json, '%s_data' % name, row)

    def add_dataframe_rows(self, name, df, hxltags=None):
        if hxltags:
            df = df.rename(columns=hxltags)
        self.json['%s_data' % name] = df.to_dict(orient='records')

    def add_data_rows_by_key(self, name, countryiso, rows, hxltags=None):
        fullname = '%s_data' % name
        jsondict = self.json.get(fullname, dict())
        jsondict[countryiso] = list()
        for row in rows:
            if hxltags:
                newrow = dict()
                for header, hxltag in hxltags.items():
                    newrow[hxltag] = row[header]
            else:
                newrow = row
            jsondict[countryiso].append(newrow)
        self.json[fullname] = jsondict

    def generate_json(self, key, rows):
        hxltags = rows[1]
        for row in rows[2:]:
            newrow = dict()
            for i, hxltag in enumerate(hxltags):
                value = row[i]
                if value in [None, '']:
                    continue
                newrow[hxltag] = str(value)
            self.add_data_row(key, newrow)

    def update_tab(self, tabname, values):
        if tabname not in self.updatetabs:
            return
        self.generate_json(tabname, values)

    def add_additional_json(self, downloader):
        for datasetinfo in self.json_configuration.get('additional_json', list()):
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
                self.add_data_row(name, newrow)

    def save(self, folder=None):
        filepath = self.json_configuration['filepath']
        if folder:
            filepath = join(folder, filepath)
        logger.info('Writing JSON to %s' % filepath)
        save_json(self.json, filepath)
        additional = self.json_configuration.get('additional', list())
        for filedetails in additional:
            filedetailspath = filedetails['filepath']
            if folder:
                filedetailspath = join(folder, filepath)
            save_json(self.json['%s_data' % filedetails['key']], filedetailspath)
        return filepath
