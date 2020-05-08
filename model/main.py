# -*- coding: utf-8 -*-
from hdx.location.country import Country

from model.fts import get_fts
from model.humaccess import get_humaccess
from model.tabular_hdx import get_tabular_hdx
from model.who import get_who


def get_indicators(configuration, downloader):
    national = [['iso3', 'countryname'], ['#country+code', '#country+name']]
    subnational = []
    countries = configuration['countries']
    who_headers, who_columns = get_who(configuration, countries, downloader)
    tabular_headers, tabular_columns = get_tabular_hdx(configuration, countries, downloader)
    fts_headers, fts_columns = get_fts(configuration, countries, downloader)
    humaccess_headers, humaccess_columns = get_humaccess(configuration, countries, downloader)
    for i, header in enumerate(national):
        header.extend(who_headers[i])
        header.extend(tabular_headers[i])
        header.extend(fts_headers[i])
        header.extend(humaccess_headers[i])

    for i, countryiso in enumerate(countries):
        row = [countryiso, Country.get_country_name_from_iso3(countryiso)]
        for column in who_columns:
            row.append(column[countryiso])
        for column in tabular_columns:
            val = column.get(countryiso)
            row.append(val)
        for column in fts_columns:
            row.append(column[countryiso])
        for column in humaccess_columns:
            if countryiso in column:
                row.append(column[countryiso])
        national.append(row)

    return national, subnational
