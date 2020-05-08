# -*- coding: utf-8 -*-
from hdx.location.country import Country

from model.tabular_hdx import get_tabular_hdx
from model.who import get_who


def get_indicators(configuration, downloader):
    national = [['iso3', 'countryname'], ['#country+code', '#country+name']]
    subnational = []
    countries = configuration['countries']
    who_headers, who_columns = get_who(configuration, countries, downloader)
    tabular_headers, tabular_columns = get_tabular_hdx(configuration, countries, downloader)
    for i, header in enumerate(national):
        header.extend(who_headers[i])
        header.extend(tabular_headers[i])

    for i, countryiso in enumerate(countries):
        row = [countryiso, Country.get_country_name_from_iso3(countryiso)]
        for column in who_columns:
            row.append(column[countryiso])
        for column in tabular_columns:
            val = column.get(countryiso)
            row.append(val)
        national.append(row)

    return national, subnational
