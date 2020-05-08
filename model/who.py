# -*- coding: utf-8 -*-

from jsonpath_rw import parse

from model import RowParser


def get_who(configuration, countries, downloader):
    url = configuration['who_url']
    response = downloader.download(url)
    json = response.json()
    expression = parse('features[*].attributes')
    cases = dict()
    deaths = dict()
    rowparser = RowParser(countries, {'iso3_col': 'ISO_3_CODE', 'date_col': 'date_epicrv', 'date_type': 'int'})
    for result in expression.find(json):
        row = result.value
        countryiso = rowparser.do_set_value(row)
        if countryiso:
            cases[countryiso] = row['CumCase']
            deaths[countryiso] = row['CumDeath']
    return [['NoCases', 'NoDeaths'], ['#affected+infected', '#affected+killed']], [cases, deaths]



