# -*- coding: utf-8 -*-
from datetime import datetime

from hdx.utilities.dateparse import parse_date


def get_who(configuration, countries, downloader):
    url = configuration['who_url']
    headers, iterator = downloader.get_tabular_rows(url, headers=1, dict_form=True, format='csv')
    cases = dict()
    deaths = dict()
    max_date = parse_date('1900-01-01 00:00:00+00:00')
    for row in iterator:
        countryiso = row['ISO_3_CODE']
        if countryiso not in countries:
            continue
        date = parse_date(row['date_epicrv'])
        if date >= max_date:
            max_date = date
            cases[countryiso] = row['CumCase']
            deaths[countryiso] = row['CumDeath']
    return cases, deaths



