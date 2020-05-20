# -*- coding: utf-8 -*-
from hdx.location.country import Country

from model.admininfo import AdminInfo
from model.fts import get_fts
from model.humaccess import get_humaccess
from model.ipc import get_ipc
from model.tabularparser import get_tabular
from model.whowhatwhere import get_whowhatwhere


def get_indicators(configuration, downloader):
    national = [['iso3', 'countryname'], ['#country+code', '#country+name']]
    subnational = [['iso3', 'countryname', 'adm1_pcode', 'adm1_name'], ['#country+code', '#country+name', '#adm1+code', '#adm1+name']]
    sources = [['Indicator', 'Date', 'Source', 'Url'], ['#indicator+name', '#date', '#meta+source', '#meta+url']]

    admininfo = AdminInfo.get()
    countryiso3s = admininfo.countryiso3s
    tabular_headers, tabular_columns, tabular_sources = get_tabular(configuration, [countryiso3s], 'national', downloader)
    fts_headers, fts_columns, fts_sources = get_fts(configuration, countryiso3s, downloader)
    humaccess_headers, humaccess_columns, humaccess_sources = get_humaccess(configuration, countryiso3s, downloader)
    for i, header in enumerate(national):
        header.extend(tabular_headers[i])
        header.extend(fts_headers[i])
        header.extend(humaccess_headers[i])

    for i, countryiso3 in enumerate(countryiso3s):
        countryname = Country.get_country_name_from_iso3(countryiso3)
        row = [countryiso3, countryname]
        for column in tabular_columns:
            row.append(column.get(countryiso3))
        for column in fts_columns:
            row.append(column[countryiso3])
        for column in humaccess_columns:
            row.append(column.get(countryiso3))
        national.append(row)

    sources.extend(tabular_sources)
    sources.extend(fts_sources)
    sources.extend(humaccess_sources)

    pcodes = admininfo.pcodes
    tabular_headers, tabular_columns, tabular_sources = get_tabular(configuration, [countryiso3s, pcodes], 'subnational', downloader)
    ipc_headers, ipc_columns, ipc_sources = get_ipc(configuration, admininfo, downloader)
    whowhatwhere_headers, whowhatwhere_columns, whowhatwhere_sources = get_whowhatwhere(configuration, admininfo, downloader)
    for i, header in enumerate(subnational):
        header.extend(tabular_headers[i])
        header.extend(ipc_headers[i])
        header.extend(whowhatwhere_headers[i])

    for i, pcode in enumerate(pcodes):
        countryiso3 = admininfo.pcode_to_iso3[pcode]
        countryname = Country.get_country_name_from_iso3(countryiso3)
        adm1_name = admininfo.pcode_to_name[pcode]
        row = [countryiso3, countryname, pcode, adm1_name]
        for column in tabular_columns:
            row.append(column.get(pcode))
        for column in ipc_columns:
            row.append(column.get(pcode))
        for column in whowhatwhere_columns:
            row.append(column.get(pcode))
        subnational.append(row)

    sources.extend(tabular_sources)
    sources.extend(ipc_sources)
    sources.extend(whowhatwhere_sources)

    return national, subnational, sources
