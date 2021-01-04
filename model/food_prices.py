# -*- coding: utf-8 -*-
import logging

from hdx.location.country import Country

from model import calculate_ratios
from utilities.readers import read_hdx

logger = logging.getLogger(__name__)


def add_food_prices(configuration, today, countryiso3s, downloader, scrapers=None):
    name = 'food_prices'
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list()
    datasetinfo = configuration[name]
    headers, iterator = read_hdx(downloader, datasetinfo, today=today)
    allowed_months = set()
    for i in range(1, 7, 1):
        month = today.month - i
        if month > 0:
            allowed_months.add('%d/%d' % (today.year, month))
        else:
            allowed_months.add('%d/%d' % (today.year - 1, 12 + month))
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
    ratios = calculate_ratios(commods_per_country, affected_commods_per_country)
    hxltag = '#value+food+num+ratio'
    logger.info('Processed WFP')
    return [['Food Prices Ratio'], [hxltag]], [ratios], [(hxltag, datasetinfo['date'], datasetinfo['source'], datasetinfo['source_url'])]
