# -*- coding: utf-8 -*-
import logging

from hdx.location.country import Country

from model.admininfo import AdminInfo
from model.fts import get_fts
from model.humaccess import get_humaccess
from model.ipc import get_ipc
from model.tabularparser import get_tabular_hdx, get_tabular_json


def get_indicators(configuration, downloader):
    national = [['iso3', 'countryname'], ['#country+code', '#country+name']]
    subnational = [['iso3', 'countryname', 'adm1_pcode', 'adm1_name'], ['#country+code', '#country+name', '#adm1+code', '#adm1+name']]

    admininfo = AdminInfo(configuration)
    countryiso3s = admininfo.countryiso3s
    json_headers, json_columns = get_tabular_json(configuration, countryiso3s, downloader, 'national')
    tabular_headers, tabular_columns = get_tabular_hdx(configuration, countryiso3s, 'national', downloader)
    fts_headers, fts_columns = get_fts(configuration, countryiso3s, downloader)
    humaccess_headers, humaccess_columns = get_humaccess(configuration, countryiso3s, downloader)
    for i, header in enumerate(national):
        header.extend(json_headers[i])
        header.extend(tabular_headers[i])
        header.extend(fts_headers[i])
        header.extend(humaccess_headers[i])

    for i, countryiso3 in enumerate(countryiso3s):
        countryname = Country.get_country_name_from_iso3(countryiso3)
        row = [countryiso3, countryname]
        for column in json_columns:
            row.append(column[countryiso3])
        for column in tabular_columns:
            row.append(column.get(countryiso3))
        for column in fts_columns:
            row.append(column[countryiso3])
        for column in humaccess_columns:
            row.append(column.get(countryiso3))
        national.append(row)

    pcodes = admininfo.pcodes
    ipc_headers, ipc_columns = get_ipc(configuration, admininfo, downloader)
    tabular_headers, tabular_columns = get_tabular_hdx(configuration, pcodes, 'subnational', downloader)
    for i, header in enumerate(subnational):
        header.extend(ipc_headers[i])
        header.extend(tabular_headers[i])

    for i, pcode in enumerate(pcodes):
        countryiso3 = admininfo.pcode_to_iso3[pcode]
        countryname = Country.get_country_name_from_iso3(countryiso3)
        adm1_name = admininfo.pcode_to_name[pcode]
        row = [countryiso3, countryname, pcode, adm1_name]
        for column in ipc_columns:
            row.append(column.get(pcode))
        for column in tabular_columns:
            row.append(column.get(pcode))
        subnational.append(row)

    return national, subnational
