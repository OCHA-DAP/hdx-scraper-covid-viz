# -*- coding: utf-8 -*-
import logging

from model.rowparser import RowParser

logger = logging.getLogger(__name__)

hxl_lookup = {'Percentage of identified access contraints where the OCHA country office reported having an impact because of the COVID-19 outbreak': '#access+constraints'}


def get_humaccess(configuration, countryiso3s, downloader):
    url = configuration['hum_access_url']
    superheaders_temp, _ = downloader.get_tabular_rows(url, headers=1, dict_form=True, format='csv')
    headers, iterator = downloader.get_tabular_rows(url, headers=2, dict_form=True, format='csv')
    valuedicts = list()
    iso3_col = 'ISO3'
    val_cols = list()
    superheaders = dict()
    cursuperheader = None
    j = 0
    for i, header in enumerate(headers):
        if header != iso3_col and header.lower() != 'country':
            val_cols.append(header)
            valuedicts.append(dict())
            superheader = superheaders_temp[i]
            if superheader:
                superheader = superheader.split(':')[0].lower()
                if superheader != cursuperheader:
                    cursuperheader = superheader
            if cursuperheader:
                superheaders[j] = cursuperheader
            j = j + 1
    rowparser = RowParser(countryiso3s, {'adm_col': 'ISO3'})
    for row in iterator:
        countryiso = rowparser.do_set_value(row)
        if countryiso:
            for i, val_col in enumerate(val_cols):
                valuedicts[i][countryiso] = row[val_col]
    hxlheaders = list()
    cursuperheader = None
    counter = 1
    for i, val_col in enumerate(val_cols):
        hxltag = hxl_lookup.get(val_col)
        if not hxltag:
            superheader = superheaders.get(i)
            if superheader != cursuperheader:
                counter = 1
                cursuperheader = superheader
            hxltag = '#access+%s_%d' % (cursuperheader, counter)
            counter += 1
        hxlheaders.append(hxltag)
    retheaders = [val_cols, hxlheaders]
    logger.info('Processed Humanitarian Access')
    return retheaders, valuedicts



