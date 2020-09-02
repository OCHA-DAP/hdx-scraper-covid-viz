# -*- coding: utf-8 -*-
from hdx.data.dataset import Dataset
from hdx.location.country import Country

from model import today_str
from utilities import get_date_from_dataset_date
from model.access_constraints import get_access
from utilities.admininfo import AdminInfo
from model.covidtrend import get_covid_trend
from model.food_prices import add_food_prices
from model.fts import get_fts
from model.ipc import get_ipc
from utilities.regional import get_regional
from utilities.tabularparser import get_tabular
from model.unhcr import get_unhcr
from model.vaccination_campaigns import add_vaccination_campaigns
from model.whowhatwhere import get_whowhatwhere


def extend_headers(headers, *args):
    result = headers[:2]
    for i, header in enumerate(result):
        for arg in args:
            if arg:
                header.extend(arg[i])
    return result


def extend_columns(level, rows, adms, admininfo, *args):
    columns = list()
    for arg in args:
        if arg:
            columns.extend(arg)
    if adms is None:
        adms = ['global']
    for i, adm in enumerate(adms):
        if level == 'global':
            row = list()
        elif level == 'regional':
            row = [adm]
        elif level == 'national':
            row = [adm, Country.get_country_name_from_iso3(adm), '|'.join(sorted(list(admininfo.iso3_to_region_and_hrp[adm])))]
        elif level == 'subnational':
            countryiso3 = admininfo.pcode_to_iso3[adm]
            countryname = Country.get_country_name_from_iso3(countryiso3)
            adm1_name = admininfo.pcode_to_name[adm]
            row = [countryiso3, countryname, adm, adm1_name]
        for column in columns:
            row.append(column.get(adm))
        rows.append(row)
    return columns


def extend_sources(sources, *args):
    for arg in args:
        if arg:
            sources.extend(arg)


def add_population(population_lookup, headers, columns):
    population_index = headers[1].index('#population')
    if population_index != -1:
        population_lookup.update(columns[population_index])


def get_indicators(configuration, downloader, outputs, tabs, scrapers=None, basic_auths=dict()):
    world = [list(), list()]
    regional = [['regionnames'], ['#region+name']]
    national = [['iso3', 'countryname', 'region'], ['#country+code', '#country+name', '#region+name']]
    subnational = [['iso3', 'countryname', 'adm1_pcode', 'adm1_name'], ['#country+code', '#country+name', '#adm1+code', '#adm1+name']]
    sources = [('Indicator', 'Date', 'Source', 'Url'), ('#indicator+name', '#date', '#meta+source', '#meta+url')]

    admininfo = AdminInfo.setup(downloader)
    countryiso3s = admininfo.countryiso3s
    pcodes = admininfo.pcodes
    population_lookup = dict()

    def update_tab(name, data):
        for output in outputs.values():
            output.update_tab(name, data)

    if 'national' in tabs:
        fts_wheaders, fts_wcolumns, fts_wsources, fts_headers, fts_columns, fts_sources = get_fts(basic_auths, configuration, countryiso3s, scrapers)
        access_wheaders, access_wcolumns, access_wsources, access_rheaders, access_rcolumns, access_rsources, access_headers, access_columns, access_sources = get_access(configuration, admininfo, downloader, scrapers)
        food_headers, food_columns, food_sources = add_food_prices(configuration, countryiso3s, downloader, scrapers)
        campaign_headers, campaign_columns, campaign_sources = add_vaccination_campaigns(configuration, countryiso3s, downloader, outputs, scrapers)
        unhcr_headers, unhcr_columns, unhcr_sources = get_unhcr(configuration, countryiso3s, downloader, scrapers)
        tabular_headers, tabular_columns, tabular_sources = get_tabular(basic_auths, configuration, 'national', downloader, scrapers)

        national_headers = extend_headers(national, tabular_headers, food_headers, campaign_headers, fts_headers, unhcr_headers, access_headers)
        national_columns = extend_columns('national', national, countryiso3s, admininfo, tabular_columns, food_columns, campaign_columns, fts_columns, unhcr_columns, access_columns)
        extend_sources(sources, tabular_sources, food_sources, campaign_sources, fts_sources, unhcr_sources, access_sources)
        add_population(population_lookup, tabular_headers, tabular_columns)
        update_tab('national', national)

        if 'world' in tabs:
            population_lookup['H63'] = sum(population_lookup.values())
            tabular_headers, tabular_columns, tabular_sources = get_tabular(basic_auths, configuration, 'global', downloader, scrapers)
            extend_headers(world, [['Population'], ['#population']], fts_wheaders, access_wheaders, tabular_headers)
            extend_columns('global', world, None, None, [{'global': population_lookup['H63']}], fts_wcolumns, access_wcolumns, tabular_columns)
            extend_sources(sources, fts_wsources, access_wsources, tabular_sources)
            update_tab('world', world)

        if 'regional' in tabs:
            regional_headers, regional_columns = get_regional(configuration, national_headers, national_columns, admininfo)
            extend_headers(regional, regional_headers, access_rheaders)
            extend_columns('regional', regional, admininfo.regions, admininfo, regional_columns, access_rcolumns)
            extend_sources(sources, access_rsources)
            add_population(population_lookup, regional_headers, regional_columns)
            update_tab('regional', regional)

    if 'subnational' in tabs:
        ipc_headers, ipc_columns, ipc_sources = get_ipc(configuration, admininfo, downloader, scrapers)
        whowhatwhere_headers, whowhatwhere_columns, whowhatwhere_sources = get_whowhatwhere(configuration, admininfo, downloader, scrapers)
        tabular_headers, tabular_columns, tabular_sources = get_tabular(basic_auths, configuration, 'subnational', downloader, scrapers)

        extend_headers(subnational, ipc_headers, tabular_headers, whowhatwhere_headers)
        extend_columns('subnational', subnational, pcodes, admininfo, ipc_columns, tabular_columns, whowhatwhere_columns)
        extend_sources(sources, tabular_sources, ipc_sources, whowhatwhere_sources)
        update_tab('subnational', subnational)

    covid_sources = get_covid_trend(configuration, outputs, admininfo, population_lookup, scrapers)
    extend_sources(sources, covid_sources)

    admininfo.output_matches()
    admininfo.output_ignored()
    admininfo.output_errors()

    for sourceinfo in configuration['additional_sources']:
        date = sourceinfo.get('date')
        if date is None:
            if sourceinfo.get('force_date_today', False):
                date = today_str
        source = sourceinfo.get('source')
        source_url = sourceinfo.get('source_url')
        dataset_name = sourceinfo.get('dataset')
        if dataset_name:
            dataset = Dataset.read_from_hdx(dataset_name)
            if date is None:
                date = get_date_from_dataset_date(dataset)
            if source is None:
                source = dataset['dataset_source']
            if source_url is None:
                source_url = dataset.get_hdx_url()
        sources.append((sourceinfo['indicator'], date, source, source_url))

    sources = [list(elem) for elem in dict.fromkeys(sources)]
    update_tab('sources', sources)
