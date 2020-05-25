# -*- coding: utf-8 -*-
from hdx.location.country import Country

from model.admininfo import AdminInfo
from model.fts import get_fts
from model.copydata import get_copy
from model.ipc import get_ipc
from model.tabularparser import get_tabular
from model.whowhatwhere import get_whowhatwhere


def extend_headers(headers, *args):
    for i, header in enumerate(headers):
        for arg in args:
            if arg:
                header.extend(arg[i])


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


def get_indicators(configuration, downloader, scraper=None):
    world = [list(), list()]
    national = [['iso3', 'countryname'], ['#country+code', '#country+name']]
    subnational = [['iso3', 'countryname', 'adm1_pcode', 'adm1_name'], ['#country+code', '#country+name', '#adm1+code', '#adm1+name']]
    sources = [['Indicator', 'Date', 'Source', 'Url'], ['#indicator+name', '#date', '#meta+source', '#meta+url']]

    admininfo = AdminInfo.get()
    countryiso3s = admininfo.countryiso3s
    tabular_headers, tabular_columns, tabular_sources = get_tabular(configuration, [countryiso3s], 'national', downloader, scraper)
    fts_headers, fts_columns, fts_sources = get_fts(configuration, countryiso3s, downloader, scraper)
    copy_headers, copy_columns, copy_sources = get_copy(configuration, [countryiso3s], 'national', downloader, scraper)

    extend_headers(national, tabular_headers, fts_headers, copy_headers)
    extend_columns(national, countryiso3s, None, tabular_columns, fts_columns, copy_columns)
    extend_sources(sources, tabular_sources, fts_sources, copy_sources)

    pcodes = admininfo.pcodes
    tabular_headers, tabular_columns, tabular_sources = get_tabular(configuration, [countryiso3s, pcodes], 'subnational', downloader, scraper)
    ipc_headers, ipc_columns, ipc_sources = get_ipc(configuration, admininfo, downloader, scraper)
    whowhatwhere_headers, whowhatwhere_columns, whowhatwhere_sources = get_whowhatwhere(configuration, admininfo, downloader, scraper)

    extend_headers(subnational, tabular_headers, ipc_headers, whowhatwhere_headers)
    extend_columns(subnational, pcodes, admininfo, tabular_columns, ipc_columns, whowhatwhere_columns)
    extend_sources(sources, tabular_sources, ipc_sources, whowhatwhere_sources)

    return world, national, subnational, sources
