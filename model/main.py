# -*- coding: utf-8 -*-
import logging

from hdx.data.dataset import Dataset
from hdx.location.adminone import AdminOne
from hdx.location.country import Country

from model import today_str
from utilities import get_date_from_dataset_date
from model.who_covid import get_who_covid
from model.food_prices import add_food_prices
from model.fts import get_fts
from model.ipc import get_ipc
from utilities.region import Region
from utilities.regional import get_regional, get_world
from utilities.tabularparser import get_tabular
from model.unhcr import get_unhcr
from model.vaccination_campaigns import add_vaccination_campaigns
from model.whowhatwhere import get_whowhatwhere

logger = logging.getLogger(__name__)


def extend_headers(headers, *args):
    result = [list(), list()]
    for i, header in enumerate(headers[:2]):
        for arg in args:
            if arg:
                result[i].extend(arg[i])
                header.extend(arg[i])
    return result


def extend_columns(level, rows, adms, h25, region, adminone, headers, *args):
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
            ishrp = 'Y' if adm in h25 else 'N'
            regions = sorted(list(region.iso3_to_region_and_hrp[adm]))
            regions.remove('H63')
            row = [adm, Country.get_country_name_from_iso3(adm), ishrp, '|'.join(regions)]
        elif level == 'subnational':
            countryiso3 = adminone.pcode_to_iso3[adm]
            countryname = Country.get_country_name_from_iso3(countryiso3)
            adm1_name = adminone.pcode_to_name[adm]
            row = [countryiso3, countryname, adm, adm1_name]
        else:
            raise ValueError('Invalid level')
        append = True
        for existing_row in rows[2:]:
            match = True
            for i, col in enumerate(row):
                if existing_row[i] != col:
                    match = False
                    break
            if match:
                append = False
                row = existing_row
                break
        if append:
            for i, hxltag in enumerate(rows[1][len(row):]):
                if hxltag not in headers[1]:
                    row.append(None)
        for column in columns:
            row.append(column.get(adm))
        if append:
            rows.append(row)
    return columns


def extend_sources(sources, *args):
    for arg in args:
        if arg:
            sources.extend(arg)


def get_indicators(configuration, downloader, outputs, tabs, scrapers=None, basic_auths=dict(), use_live=True):
    world = [list(), list()]
    regional = [['regionnames'], ['#region+name']]
    national = [['iso3', 'countryname', 'ishrp', 'region'], ['#country+code', '#country+name', '#meta+ishrp', '#region+name']]
    subnational = [['iso3', 'countryname', 'adm1_pcode', 'adm1_name'], ['#country+code', '#country+name', '#adm1+code', '#adm1+name']]
    sources = [('Indicator', 'Date', 'Source', 'Url'), ('#indicator+name', '#date', '#meta+source', '#meta+url')]

    Country.countriesdata(use_live=use_live, country_name_overrides=configuration['country_name_overrides'], country_name_mappings=configuration['country_name_mappings'])

    h63 = configuration['h63']
    h25 = configuration['h25']
    configuration['countries_fuzzy_try'] = h25
    region = Region(downloader, configuration['regional'], h63, h25)
    admin1_info = list()
    for row in configuration['admin1_info']:
        newrow = {'pcode': row['ADM1_PCODE'], 'name': row['ADM1_REF'], 'iso3': row['alpha_3']}
        admin1_info.append(newrow)
    configuration['admin1_info'] = admin1_info
    adminone = AdminOne(configuration)
    pcodes = adminone.pcodes
    population_lookup = dict()

    def update_tab(name, data):
        logger.info('Updating tab: %s' % name)
        for output in outputs.values():
            output.update_tab(name, data)

    population_headers, population_columns, population_sources = get_tabular(basic_auths, configuration, h63, adminone, 'national', downloader, scrapers=['population'], population_lookup=population_lookup)
    national_headers = extend_headers(national, population_headers)
    national_columns = extend_columns('national', national, h63, h25, region, adminone, national_headers, population_columns)
    extend_sources(sources, population_sources)
    population_lookup['H63'] = sum(population_lookup.values())
    population_headers, population_columns = get_regional(configuration, region, national_headers, national_columns,
                                                          population_lookup=population_lookup)
    regional_headers = extend_headers(regional, population_headers)
    extend_columns('regional', regional, region.regions, h25, region, adminone, regional_headers, population_columns)
    population_headers, population_columns, population_sources = get_tabular(basic_auths, configuration, h63, adminone, 'subnational', downloader, scrapers=['population'], population_lookup=population_lookup)
    subnational_headers = extend_headers(subnational, population_headers)
    extend_columns('subnational', subnational, pcodes, h25, region, adminone, subnational_headers, population_columns)
    covid_wheaders, covid_wcolumns, covid_h63columns, covid_headers, covid_columns, covid_sources = get_who_covid(configuration, outputs, h25, h63, region, population_lookup, scrapers)
    extend_sources(sources, covid_sources)

    ipc_headers, ipc_columns, ipc_sheaders, ipc_scolumns, ipc_sources = get_ipc(configuration, h63, adminone, downloader, scrapers)
    if 'national' in tabs:
        fts_wheaders, fts_wcolumns, fts_wsources, fts_headers, fts_columns, fts_sources = get_fts(basic_auths, configuration, h63, scrapers)
        food_headers, food_columns, food_sources = add_food_prices(configuration, h63, downloader, scrapers)
        campaign_headers, campaign_columns, campaign_sources = add_vaccination_campaigns(configuration, h63, downloader, outputs, scrapers)
        unhcr_headers, unhcr_columns, unhcr_sources = get_unhcr(configuration, h63, downloader, scrapers)
        tabular_headers, tabular_columns, tabular_sources = get_tabular(basic_auths, configuration, h63, adminone, 'national', downloader, scrapers=scrapers, population_lookup=population_lookup)

        national_headers = extend_headers(national, covid_headers, tabular_headers, food_headers, campaign_headers, fts_headers, unhcr_headers, ipc_headers)
        national_columns = extend_columns('national', national, h63, h25, region, adminone, national_headers, covid_columns, tabular_columns, food_columns, campaign_columns, fts_columns, unhcr_columns, ipc_columns)
        extend_sources(sources, tabular_sources, food_sources, campaign_sources, fts_sources, unhcr_sources)
        update_tab('national', national)

        if 'regional' in tabs:
            regional_headers, regional_columns = get_regional(configuration, region, national_headers,
                                                              national_columns, None, (covid_wheaders, covid_wcolumns),
                                                              (fts_wheaders, fts_wcolumns))
            regional_headers = extend_headers(regional, regional_headers)
            extend_columns('regional', regional, region.regions + ['global'], h25, region, adminone, regional_headers, regional_columns)
            update_tab('regional', regional)

            if 'world' in tabs:
                rgheaders, rgcolumns = get_world(configuration, regional_headers, regional_columns)
                tabular_headers, tabular_columns, tabular_sources = get_tabular(basic_auths, configuration, h63, adminone, 'global', downloader, scrapers=scrapers, population_lookup=population_lookup)
                world_headers = extend_headers(world, covid_wheaders, fts_wheaders, tabular_headers, rgheaders)
                extend_columns('global', world, None, None, region, adminone, world_headers, covid_h63columns, fts_wcolumns, tabular_columns, rgcolumns)
                extend_sources(sources, fts_wsources, tabular_sources)
                update_tab('world', world)

    if 'subnational' in tabs:
        whowhatwhere_headers, whowhatwhere_columns, whowhatwhere_sources = get_whowhatwhere(configuration, adminone, downloader, scrapers)
        tabular_headers, tabular_columns, tabular_sources = get_tabular(basic_auths, configuration, h63, adminone, 'subnational', downloader, scrapers=scrapers, population_lookup=population_lookup)

        subnational_headers = extend_headers(subnational, ipc_sheaders, tabular_headers, whowhatwhere_headers)
        extend_columns('subnational', subnational, pcodes, h25, region, adminone, subnational_headers, ipc_scolumns, tabular_columns, whowhatwhere_columns)
        extend_sources(sources, tabular_sources, whowhatwhere_sources)
        update_tab('subnational', subnational)
    extend_sources(sources, ipc_sources)

    adminone.output_matches()
    adminone.output_ignored()
    adminone.output_errors()

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
    return configuration['h25']
