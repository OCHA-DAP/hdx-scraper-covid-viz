import logging

from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add

from model import get_percent
from model.tabularparser import get_tabular_source

logger = logging.getLogger(__name__)


def get_ipc(configuration, admininfo, downloader):
    url = configuration['ipc_url']
    phasedict = dict()
    popdict = dict()
    for countryiso3 in admininfo.countryiso3s:
        countryiso2 = Country.get_iso2_from_iso3(countryiso3)
        data = get_tabular_source(downloader, {'url': url % countryiso2, 'sheetname': 'IPC', 'headers': [4, 6], 'format': 'xlsx'}, fill_merged_cells=True)
        data = list(data)
        adm1_names = set()
        for row in data:
            area = row['Area']
            if not area or area == row['Country']:
                continue
            adm1_name = row['Level 1 Name']
            if adm1_name:
                adm1_names.add(adm1_name)
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
            pcode = admininfo.get_pcode(countryiso3, adm1_name)
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
    dataset = Dataset.read_from_hdx(configuration['ipc_dataset'])
    date = parse_date(dataset['last_modified']).strftime('%Y-%m-%d')
    return [['FoodInsecurityP3+'], ['#affected+food+p3+pct']], [phasedict], \
           [['#affected+food+p3+pct', date, dataset['dataset_source'], dataset.get_hdx_url()]]
