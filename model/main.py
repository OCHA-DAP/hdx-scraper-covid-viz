# -*- coding: utf-8 -*-
import logging

from hdx.data.dataset import Dataset
from hdx.location.adminone import AdminOne
from hdx.location.country import Country
from hdx.scraper import get_date_from_dataset_date
from hdx.scraper.scrapers import run_scrapers

from model.covax_deliveries import get_covax_deliveries
from model.inform import get_inform
from model.iom_dtm import get_iom_dtm
from model.who_covid import get_who_covid
from model.food_prices import add_food_prices
from model.fts import get_fts
from model.ipc import get_ipc
from utilities.region import Region
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


def extend_columns(level, rows, adms, hrp_countries, region, adminone, headers, *args):
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
            ishrp = 'Y' if adm in hrp_countries else 'N'
            regions = sorted(list(region.iso3_to_region_and_hrp[adm]))
            regions.remove('GHO')
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


def get_indicators(configuration, today, downloader, outputs, tabs, scrapers=None, basic_auths=dict(), other_auths=dict(), countries_override=None, use_live=True):
    world = [list(), list()]
    regional = [['regionnames'], ['#region+name']]
    national = [['iso3', 'countryname', 'ishrp', 'region'], ['#country+code', '#country+name', '#meta+ishrp', '#region+name']]
    subnational = [['iso3', 'countryname', 'adm1_pcode', 'adm1_name'], ['#country+code', '#country+name', '#adm1+code', '#adm1+name']]
    sources = [('Indicator', 'Date', 'Source', 'Url'), ('#indicator+name', '#date', '#meta+source', '#meta+url')]

    Country.countriesdata(use_live=use_live, country_name_overrides=configuration['country_name_overrides'], country_name_mappings=configuration['country_name_mappings'])

    today_str = today.strftime('%Y-%m-%d')
    if countries_override:
        gho_countries = countries_override
        hrp_countries = countries_override
    else:
        gho_countries = configuration['gho']
        hrp_countries = configuration['HRPs']
    configuration['countries_fuzzy_try'] = hrp_countries
    region = Region(configuration['regional'], today, downloader, gho_countries, hrp_countries)
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

    population_headers, population_columns, population_sources = run_scrapers(configuration, gho_countries, adminone, 'national', downloader, basic_auths, today=today, today_str=today_str, scrapers=['population'], population_lookup=population_lookup)
    national_headers = extend_headers(national, population_headers)
    national_columns = extend_columns('national', national, gho_countries, hrp_countries, region, None, national_headers, population_columns)
    extend_sources(sources, population_sources)
    population_lookup['GHO'] = sum(population_lookup.values())
    population_headers, population_columns = region.get_regional(region, national_headers, national_columns,
                                                                 population_lookup=population_lookup)
    regional_headers = extend_headers(regional, population_headers)
    extend_columns('regional', regional, region.regions, None, region, None, regional_headers, population_columns)
    population_headers, population_columns, population_sources = run_scrapers(configuration, gho_countries, adminone, 'subnational', downloader, basic_auths, today=today, today_str=today_str, scrapers=['population'], population_lookup=population_lookup)
    subnational_headers = extend_headers(subnational, population_headers)
    extend_columns('subnational', subnational, pcodes, None, None, adminone, subnational_headers, population_columns)
    covid_wheaders, covid_wcolumns, covid_ghocolumns, covid_headers, covid_columns, covid_sources = get_who_covid(configuration, today, outputs, hrp_countries, gho_countries, region, population_lookup, scrapers)
    extend_sources(sources, covid_sources)

    ipc_headers, ipc_columns, ipc_sheaders, ipc_scolumns, ipc_sources = get_ipc(configuration, today, gho_countries, adminone, downloader, scrapers)
    if 'national' in tabs:
        fts_wheaders, fts_wcolumns, fts_wsources, fts_headers, fts_columns, fts_sources = get_fts(basic_auths, configuration, today, today_str, gho_countries, scrapers)
        food_headers, food_columns, food_sources = add_food_prices(configuration, today, gho_countries, downloader, scrapers)
        campaign_headers, campaign_columns, campaign_sources = add_vaccination_campaigns(configuration, today, gho_countries, downloader, outputs, scrapers)
        unhcr_headers, unhcr_columns, unhcr_sources = get_unhcr(configuration, today, today_str, gho_countries, downloader, scrapers)
        inform_headers, inform_columns, inform_sources = get_inform(configuration, today, gho_countries, other_auths, scrapers)
        covax_headers, covax_columns, covax_sources = get_covax_deliveries(configuration, today, gho_countries, downloader, scrapers)
        tabular_headers, tabular_columns, tabular_sources = run_scrapers(configuration, gho_countries, adminone, 'national', downloader, basic_auths, today=today, today_str=today_str, scrapers=scrapers, population_lookup=population_lookup)

        national_headers = extend_headers(national, covid_headers, tabular_headers, food_headers, campaign_headers, fts_headers, unhcr_headers, inform_headers, ipc_headers, covax_headers)
        national_columns = extend_columns('national', national, gho_countries, hrp_countries, region, None, national_headers, covid_columns, tabular_columns, food_columns, campaign_columns, fts_columns, unhcr_columns, inform_columns, ipc_columns, covax_columns)
        extend_sources(sources, tabular_sources, food_sources, campaign_sources, fts_sources, unhcr_sources, inform_sources, covax_sources)
        update_tab('national', national)

        if 'regional' in tabs:
            regional_headers, regional_columns = region.get_regional(region, national_headers, national_columns, None,
                                                                     (covid_wheaders, covid_wcolumns), (fts_wheaders, fts_wcolumns))
            regional_headers = extend_headers(regional, regional_headers)
            extend_columns('regional', regional, region.regions + ['global'], None, region, None, regional_headers, regional_columns)
            update_tab('regional', regional)

            if 'world' in tabs:
                rgheaders, rgcolumns = region.get_world(regional_headers, regional_columns)
                tabular_headers, tabular_columns, tabular_sources = run_scrapers(configuration, gho_countries, adminone, 'global', downloader, basic_auths, today=today, today_str=today_str, scrapers=scrapers, population_lookup=population_lookup)
                world_headers = extend_headers(world, covid_wheaders, fts_wheaders, tabular_headers, rgheaders)
                extend_columns('global', world, None, None, None, None, world_headers, covid_ghocolumns, fts_wcolumns, tabular_columns, rgcolumns)
                extend_sources(sources, fts_wsources, tabular_sources)
                update_tab('world', world)

    if 'subnational' in tabs:
        whowhatwhere_headers, whowhatwhere_columns, whowhatwhere_sources = get_whowhatwhere(configuration, today_str, adminone, downloader, scrapers)
        iomdtm_headers, iomdtm_columns, iomdtm_sources = get_iom_dtm(configuration, today_str, adminone, downloader, scrapers)
        tabular_headers, tabular_columns, tabular_sources = run_scrapers(configuration, gho_countries, adminone, 'subnational', downloader, basic_auths, today=today, today_str=today_str, scrapers=scrapers, population_lookup=population_lookup)

        subnational_headers = extend_headers(subnational, ipc_sheaders, tabular_headers, whowhatwhere_headers, iomdtm_headers)
        extend_columns('subnational', subnational, pcodes, None, None, adminone, subnational_headers, ipc_scolumns, tabular_columns, whowhatwhere_columns, iomdtm_columns)
        extend_sources(sources, tabular_sources, whowhatwhere_sources, iomdtm_sources)
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
                date = get_date_from_dataset_date(dataset, today=today)
            if source is None:
                source = dataset['dataset_source']
            if source_url is None:
                source_url = dataset.get_hdx_url()
        sources.append((sourceinfo['indicator'], date, source, source_url))

    sources = [list(elem) for elem in dict.fromkeys(sources)]
    update_tab('sources', sources)
    return hrp_countries
