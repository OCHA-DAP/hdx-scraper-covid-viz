# -*- coding: utf-8 -*-
from model.rowparser import RowParser

hxl_lookup = {'Global Assessment': '#access+assessment+pct', 'Constraints': '#access+constraints', 'Impact': '#access+impact'}


def get_humaccess(configuration, countryiso3s, downloader):
    url = configuration['hum_access_url']
    headers, iterator = downloader.get_tabular_rows(url, headers=2, dict_form=True, format='csv')
    valuedicts = list()
    iso3_col = 'ISO3'
    val_cols = list()
    for header in headers:
        if header != iso3_col and header.lower() != 'country':
            val_cols.append(header)
            valuedicts.append(dict())
    rowparser = RowParser(countryiso3s, {'adm_col': 'ISO3'})
    for row in iterator:
        countryiso = rowparser.do_set_value(row)
        if countryiso:
            for i, val_col in enumerate(val_cols):
                valuedicts[i][countryiso] = row[val_col]
    retheaders = [val_cols, [hxl_lookup.get(x, '#access+label') for x in val_cols]]
    return retheaders, valuedicts



