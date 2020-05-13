import logging

import numpy
import pandas
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add

from model import get_percent

logger = logging.getLogger(__name__)


def get_ipc(configuration, admininfo, downloader):
    url = configuration['ipc_url']
    phasedict = dict()
    popdict = dict()
    for countryiso3 in admininfo.countryiso3s:
        countryiso2 = Country.get_iso2_from_iso3(countryiso3)
        try:
            df = pandas.read_excel(url % countryiso2, header=[9, 10, 11])
        except AttributeError:
            logger.info('No IPC data for %s!' % countryiso3)
            continue
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
        data = df.to_dict('records')

        adm1_names = set()
        for row in data:
            area = row['Area']
            if not area:
                continue
            adm1_name = row['Level 1 Name']
            if adm1_name:
                adm1_names.add(adm1_name)
        for row in data:
            if adm1_names:
                country = row['Country']
                if country not in adm1_names:
                    continue
                adm1_name = country
            else:
                adm1_name = row['Area']
                if not adm1_name:
                    continue
            pcode = admininfo.get_pcode(countryiso3, adm1_name)
            if not pcode:
                continue
            population = row['Current Phase P3+ #']
            dict_of_lists_add(popdict, pcode, population)
            percentage = row['Current Phase P3+ %']
            dict_of_lists_add(phasedict, pcode, percentage)
    for pcode in phasedict:
        percentages = phasedict[pcode]
        if len(percentages) == 1:
            phasedict[pcode] = int(percentages[0] * 100 + 0.5)
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
    return [['FoodInsecurityP3+'], ['#affected+food+p3+pct']], [phasedict]
