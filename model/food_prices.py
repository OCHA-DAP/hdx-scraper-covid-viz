# -*- coding: utf-8 -*-
import inspect
import logging

from os.path import join

from hdx.location.country import Country

from model import today_str, today, number_format
from model.readers import read_tabular

logger = logging.getLogger(__name__)


def add_food_prices(configuration, countryiso3s, downloader, scraper=None):
    name = 'food_prices'
    if scraper and scraper != name:
        return list(), list(), list()
    datasetinfo = configuration[name]
    headers, iterator = read_tabular(downloader, datasetinfo)
    allowed_months = set()
    for i in range(1, 7, 1):
        month = today.month - i
        if month > 0:
            allowed_months.add('%d/%d' % (today.year, month))
        else:
            month = 12 - month
            allowed_months.add('%d/%d' % (today.year - 1, month))
    commods_per_country = dict()
    affected_commods_per_country = dict()
    for row in iterator:
        year_month = '%s/%s' % (row['Year'], row['Month'])
        if year_month not in allowed_months:
            continue
        countryiso, _ = Country.get_iso3_country_code_fuzzy(row['Country'])
        if not countryiso or countryiso not in countryiso3s:
            continue
        commods_per_country[countryiso] = commods_per_country.get(countryiso, 0) + 1
        if row['ALPS'] != 'Normal':
            affected_commods_per_country[countryiso] = affected_commods_per_country.get(countryiso, 0) + 1
    ratios = dict()
    for countryiso in affected_commods_per_country:
        ratios[countryiso] = number_format(affected_commods_per_country[countryiso] / commods_per_country[countryiso])
    hxltag = '#value+food+num+ratio'
    logger.info('Processed WFP')
    return [['Food Prices Ratio'], [hxltag]], [ratios], [[hxltag, today_str, datasetinfo['source'], datasetinfo['url']]]
