# -*- coding: utf-8 -*-
import inspect
import logging

import numpy
import pandas
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import get_fraction_str

from utilities import get_date_from_dataset_date

logger = logging.getLogger(__name__)


def get_from_pandas(url, countryiso3):
    try:
        df = pandas.read_excel(url, header=[9, 10, 11])
    except AttributeError:
        logger.info('No IPC data for %s!' % countryiso3)
        return None
    headers = list()
    for col in list(df.columns):
        colstrs = list()
        for subcol in col:
            if 'Unnamed' in subcol:
                continue
            colstrs.append(subcol)
        column = ' '.join(colstrs)
        headers.append(column)
    df.columns = headers
    df.replace(numpy.nan, '', regex=True, inplace=True)
    return df.to_dict('records')


def get_data(url, countryiso3):
    countryiso2 = Country.get_iso2_from_iso3(countryiso3)
    for page in range(1, 3):
        data = get_from_pandas(url % (page, countryiso2), countryiso3)
        if data is None:
            continue
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


def get_ipc(configuration, admininfo, scrapers=None):
    name = inspect.currentframe().f_code.co_name
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list()
    ipc_configuration = configuration['ipc']
    url = ipc_configuration['url']
    phasedict = dict()
    popdict = dict()
    for countryiso3 in admininfo.countryiso3s:
        data, adm1_names = get_data(url, countryiso3)
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
            phasedict[pcode] = get_fraction_str(percentages[0])
        else:
            populations = popdict[pcode]
            numerator = 0
            denominator = 0
            for i, percentage in enumerate(percentages):
                population = populations[i]
                numerator += population * percentage
                denominator += population
            phasedict[pcode] = get_fraction_str(numerator, denominator)
    logger.info('Processed IPC')
    dataset = Dataset.read_from_hdx(ipc_configuration['dataset'])
    date = get_date_from_dataset_date(dataset)
    hxltag = '#affected+food+ipc+p3+pct'
    return [['FoodInsecurityIPCP3+'], [hxltag]], [phasedict], \
           [(hxltag, date, dataset['dataset_source'], dataset.get_hdx_url())]
