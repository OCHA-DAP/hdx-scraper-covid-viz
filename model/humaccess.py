# -*- coding: utf-8 -*-
import logging

from model import today_str
from model.rowparser import RowParser

logger = logging.getLogger(__name__)

hxl_lookup = {'Percentage of identified access contraints where the OCHA country office reported having an impact because of the COVID-19 outbreak': '#access+constraints'}


def get_humaccess(configuration, countryiso3s, downloader):
    url = configuration['hum_access_url']
    headers, iterator = downloader.get_tabular_rows(url, headers=2, dict_form=True, format='csv')
    valuedicts = list()
    iso3_col = 'ISO3'
    val_cols = list()
    hxltags = list()
    hxlrow = next(iterator)
    if not hxlrow:
        hxlrow = next(iterator)
    for header in headers:
        if header != iso3_col and header.lower() != 'country':
            val_cols.append(header)
            hxltags.append(hxlrow[header])
            valuedicts.append(dict())
    rowparser = RowParser(countryiso3s, {'adm_col': 'ISO3'})
    for row in iterator:
        countryiso = rowparser.do_set_value(row)
        if countryiso:
            for i, val_col in enumerate(val_cols):
                valuedicts[i][countryiso] = row[val_col]
    retheaders = [val_cols, hxltags]
    logger.info('Processed Humanitarian Access')
    return retheaders, valuedicts, [[hxltag, today_str, url] for hxltag in hxltags]




