# -*- coding: utf-8 -*-

from jsonpath_ng import parse

from model.rowparser import RowParser


def get_who(configuration, countryiso3s, downloader):
    url = configuration['who_url']
    response = downloader.download(url)
    json = response.json()
    expression = parse('features[*].attributes')
    cases = dict()
    deaths = dict()
    rowparser = RowParser(countryiso3s, {'iso3_col': 'ISO_3_CODE', 'date_col': 'date_epicrv', 'date_type': 'int'})
    for result in expression.find(json):
        row = result.value
        countryiso = rowparser.do_set_value(row)
        if countryiso:
            cases[countryiso] = row['CumCase']
            deaths[countryiso] = row['CumDeath']
    return [['NoCases', 'NoDeaths'], ['#affected+infected', '#affected+killed']], [cases, deaths]



