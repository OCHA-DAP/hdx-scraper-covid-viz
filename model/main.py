# -*- coding: utf-8 -*-
from hdx.location.country import Country

from model.admininfo import AdminInfo
from model.fts import get_fts
from model.copydata import get_copy
from model.ipc import get_ipc
from model.tabularparser import get_tabular
from model.timeseriesparser import get_timeseries
from model.whowhatwhere import get_whowhatwhere


def extend_headers(headers, *args):
    for i, header in enumerate(headers):
        for arg in args:
            if arg:
                header.extend(arg[i])


def extend_wcolumns(rows, *args):
    row = list()
    for arg in args:
        if arg:
            for column in arg:
                row.append(column)
    rows.append(row)


def extend_columns(rows, adms, admininfo, *args):
    for i, adm in enumerate(adms):
        if admininfo:
            countryiso3 = admininfo.pcode_to_iso3[adm]
            countryname = Country.get_country_name_from_iso3(countryiso3)
            adm1_name = admininfo.pcode_to_name[adm]
            row = [countryiso3, countryname, adm, adm1_name]
        else:
            row = [adm, Country.get_country_name_from_iso3(adm)]
        for arg in args:
            if arg:
                for column in arg:
                    row.append(column.get(adm))
        rows.append(row)


def extend_sources(sources, *args):
    for arg in args:
        if arg:
            sources.extend(arg)


def get_indicators(configuration, downloader, sheets, scraper=None):
    world = [list(), list()]
    national = [['iso3', 'countryname'], ['#country+code', '#country+name']]
    nationaltimeseries = [['iso3', 'date', 'indicator', 'value'], ['#country+code', '#date', '#indicator+name', '#indicator+value+num']]
    subnational = [['iso3', 'countryname', 'adm1_pcode', 'adm1_name'], ['#country+code', '#country+name', '#adm1+code', '#adm1+name']]
    sources = [['Indicator', 'Date', 'Source', 'Url'], ['#indicator+name', '#date', '#meta+source', '#meta+url']]

    admininfo = AdminInfo.get()
    countryiso3s = admininfo.countryiso3s

    if 'world' in sheets or 'national' in sheets:
        fts_wheaders, fts_wcolumns, fts_wsources, fts_headers, fts_columns, fts_sources = get_fts(configuration, countryiso3s, downloader, scraper)

        extend_headers(world, fts_wheaders)
        extend_wcolumns(world, fts_wcolumns)
        extend_sources(sources, fts_wsources)

        if 'national' in sheets:
            tabular_headers, tabular_columns, tabular_sources = get_tabular(configuration, [countryiso3s], 'national', downloader, scraper)
            copy_headers, copy_columns, copy_sources = get_copy(configuration, [countryiso3s], 'national', downloader, scraper)

            extend_headers(national, tabular_headers, fts_headers, copy_headers)
            extend_columns(national, countryiso3s, None, tabular_columns, fts_columns, copy_columns)
            extend_sources(sources, tabular_sources, fts_sources, copy_sources)

    if 'national_timeseries' in sheets:
        timeseries_sources = get_timeseries(nationaltimeseries, configuration, [countryiso3s], 'national', downloader, scraper)
        extend_sources(sources, timeseries_sources)

    if 'subnational' in sheets:
        pcodes = admininfo.pcodes
        tabular_headers, tabular_columns, tabular_sources = get_tabular(configuration, [countryiso3s, pcodes], 'subnational', downloader, scraper)
        ipc_headers, ipc_columns, ipc_sources = get_ipc(configuration, admininfo, downloader, scraper)
        whowhatwhere_headers, whowhatwhere_columns, whowhatwhere_sources = get_whowhatwhere(configuration, admininfo, downloader, scraper)

        extend_headers(subnational, tabular_headers, ipc_headers, whowhatwhere_headers)
        extend_columns(subnational, pcodes, admininfo, tabular_columns, ipc_columns, whowhatwhere_columns)
        extend_sources(sources, tabular_sources, ipc_sources, whowhatwhere_sources)

    return world, national, nationaltimeseries, subnational, sources
