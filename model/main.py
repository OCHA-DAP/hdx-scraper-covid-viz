# -*- coding: utf-8 -*-
from hdx.location.country import Country

from model.who import get_who


def get_indicators(configuration, downloader):
    national = [['iso3', 'countryname'], ['#country+code', '#country+name']]
    subnational = []
    countries = configuration['countries']
    who_columns = get_who(configuration, countries, downloader)

    for i, countryiso in enumerate(countries):
        row = [countryiso, Country.get_country_name_from_iso3(countryiso)]
        for column in who_columns:
            row.append(column[countryiso])
        national.append(row)

    return national, subnational
