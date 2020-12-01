# -*- coding: utf-8 -*-
import inspect
import logging

from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import get_fraction_str

from utilities import get_date_from_dataset_date
from utilities.readers import read_tabular
from datetime import datetime
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)


def get_data(downloader, url, countryiso2):
    for page in range(1, 3):
        _, data = read_tabular(downloader, {'url': url % (page, countryiso2), 'sheet': 'IPC', 'headers': [4, 6],
                                            'format': 'xlsx'}, fill_merged_cells=True)
        data = list(data)
        adm1_names = set()
        percentages = list()
        found_data = False
        for row in data:
            area = row['Area']
            if any(v is not None for v in [row['Current Phase P3+ %'], row['First Projection Phase P3+ %'],
                                           row['Second Projection Phase P3+ %']]):
                found_data = True
            if not area or area == row['Country']:
                continue
            adm1_name = row['Level 1 Name']
            if adm1_name:
                adm1_names.add(adm1_name)
        if found_data is True:
            return data, adm1_names
    return None, None


def get_period(row, projections):
    today = datetime.today().date()
    analysis_period = ''
    for projection in projections:
        current_period = row[f'{projection} Analysis Period']
        if current_period == '':
            continue
        start = datetime.strptime(current_period[0:8], '%b %Y').date()
        end = datetime.strptime(current_period[11:19], '%b %Y').date()
        end = end + relativedelta(day=31)
        if today < end:
            analysis_period = projection
            break
    if analysis_period == '':
        for projection in reversed(projections):
            if row[f'{projection} Analysis Period'] != '':
                analysis_period = projection
                break
    return analysis_period, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')


def get_ipc(configuration, admininfo, downloader, scrapers=None):
    name = inspect.currentframe().f_code.co_name
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list(), list(), list()
    ipc_configuration = configuration['ipc']
    url = ipc_configuration['url']
    phases = ['3', '4', '5', 'P3+']
    projections = ['Current', 'First Projection', 'Second Projection']
    national_phases = {phase: dict() for phase in phases}
    national_analysed = dict()
    national_period = dict()
    national_start = dict()
    national_end = dict()
    subnational_phases = {phase: dict() for phase in phases}
    subnational_populations = {phase: dict() for phase in phases}
    for countryiso3 in admininfo.countryiso3s:
        countryiso2 = Country.get_iso2_from_iso3(countryiso3)
        data, adm1_names = get_data(downloader, url, countryiso2)
        if not data:
            continue
        row = data[0]
        analysis_period, start, end = get_period(row, projections)
        for phase in phases:
            national_phases[phase][countryiso3] = row[f'{analysis_period} Phase {phase} %']
        population_analysed = row["Current Population Analysed % of total county Pop"]
        if population_analysed != '':
            population_analysed = f'{population_analysed:.03f}'
        national_analysed[countryiso3] = population_analysed
        national_period[countryiso3] = analysis_period
        national_start[countryiso3] = start
        national_end[countryiso3] = end
        for row in data[1:]:
            country = row['Country']
            if adm1_names:
                if country not in adm1_names:
                    continue
                adm1_name = country
            else:
                adm1_name = row['Area']
                if not adm1_name or adm1_name == country:
                    continue
            pcode, _ = admininfo.get_pcode(countryiso3, adm1_name, 'IPC')
            if not pcode:
                continue
            for phase in phases:
                population = row[f'{analysis_period} Phase {phase} #']
                if population:
                    dict_of_lists_add(subnational_populations[phase], pcode, population)
                percentage = row[f'{analysis_period} Phase {phase} %']
                if percentage:
                    dict_of_lists_add(subnational_phases[phase], pcode, percentage)
    for phase in phases:
        subnational_phase = subnational_phases[phase]
        for pcode in subnational_phase:
            percentages = subnational_phase[pcode]
            if len(percentages) == 1:
                subnational_phase[pcode] = get_fraction_str(percentages[0])
            else:
                populations = subnational_populations[phase][pcode]
                numerator = 0
                denominator = 0
                for i, percentage in enumerate(percentages):
                    population = populations[i]
                    numerator += population * percentage
                    denominator += population
                subnational_phase[pcode] = get_fraction_str(numerator, denominator)
    logger.info('Processed IPC')
    dataset = Dataset.read_from_hdx(ipc_configuration['dataset'])
    date = get_date_from_dataset_date(dataset)
    headers = [f'FoodInsecurityIPC{phase}' for phase in phases]
    headers.append('FoodInsecurityIPCAnalysed')
    headers.append('FoodInsecurityIPCAnalysisPeriod')
    headers.append('FoodInsecurityIPCAnalysisPeriodStart')
    headers.append('FoodInsecurityIPCAnalysisPeriodEnd')
    hxltags = [f'#affected+food+ipc+p{phase}+pct' for phase in phases[:-1]]
    hxltags.append('#affected+food+ipc+p3plus+pct')
    hxltags.append('#affected+food+ipc+analysed+pct')
    hxltags.append('#date+ipc+period')
    hxltags.append('#date+ipc+start')
    hxltags.append('#date+ipc+end')
    national_outputs = [national_phases[phase] for phase in phases]
    national_outputs.append(national_analysed)
    national_outputs.append(national_period)
    national_outputs.append(national_start)
    national_outputs.append(national_end)
    subnational_outputs = [subnational_phases[phase] for phase in phases]
    return [headers, hxltags], national_outputs, [headers[:-4], hxltags[:-4]], subnational_outputs, \
           [(hxltag, date, dataset['dataset_source'], dataset.get_hdx_url()) for hxltag in hxltags]

