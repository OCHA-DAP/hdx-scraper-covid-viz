# -*- coding: utf-8 -*-
import inspect
import logging

from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add

from model import get_percent, get_date_from_dataset_date
from model.readers import read_tabular

logger = logging.getLogger(__name__)


def get_data(downloader, url, countryiso2):
    for page in range(1, 3):
        _, data = read_tabular(downloader, {'url': url % (page, countryiso2), 'sheet': 'IPC', 'headers': [4, 6],
                                            'format': 'xlsx'}, fill_merged_cells=True)
        data = list(data)
        adm1_names = set()
        percentages = list()
        for row in data:
            area = row['Area']
            percentage = row['Current Phase P3+ %']
            if percentage:
                percentages.append(percentage)
            if not area or area == row['Country']:
                continue
            adm1_name = row['Level 1 Name']
            if adm1_name:
                adm1_names.add(adm1_name)
        if len(percentages) != 0:
            return data, adm1_names
    return None, None


def get_ipc(configuration, admininfo, downloader, scraper=None):
    if scraper and scraper not in inspect.currentframe().f_code.co_name:
        return list(), list(), list()
    ipc_configuration = configuration['ipc']
    url = ipc_configuration['url']
    phasedict = dict()
    popdict = dict()
    for countryiso3 in admininfo.countryiso3s:
        countryiso2 = Country.get_iso2_from_iso3(countryiso3)
        data, adm1_names = get_data(downloader, url, countryiso2)
        if not data:
            continue
        for row in data:
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
            population = row['Current Phase P3+ #']
            if population:
                dict_of_lists_add(popdict, pcode, population)
            percentage = row['Current Phase P3+ %']
            if percentage:
                dict_of_lists_add(phasedict, pcode, percentage)
    for pcode in phasedict:
        percentages = phasedict[pcode]
        if len(percentages) == 1:
            phasedict[pcode] = get_percent(percentages[0])
        else:
            populations = popdict[pcode]
            numerator = 0
            denominator = 0
            for i, percentage in enumerate(percentages):
                population = populations[i]
                numerator += population * percentage
                denominator += population
            phasedict[pcode] = get_percent(numerator, denominator)
    logger.info('Processed IPC')
    dataset = Dataset.read_from_hdx(ipc_configuration['dataset'])
    date = get_date_from_dataset_date(dataset)
    hxltag = '#affected+food+ipc+p3+pct'
    return [['FoodInsecurityIPCP3+'], [hxltag]], [phasedict], \
           [(hxltag, date, dataset['dataset_source'], dataset.get_hdx_url())]
