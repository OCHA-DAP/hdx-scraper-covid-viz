# -*- coding: utf-8 -*-
import logging

from hdx.scraper.readers import read_hdx

from model import calculate_ratios

logger = logging.getLogger(__name__)


def add_vaccination_campaigns(configuration, today, countryiso3s, downloader, outputs, scrapers=None):
    name = 'vaccination_campaigns'
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list()
    datasetinfo = configuration[name]
    headers, iterator = read_hdx(downloader, datasetinfo, today=today)
    hxlrow = next(iterator)
    campaigns_per_country = dict()
    affected_campaigns_per_country = dict()
    for row in iterator:
        newrow = dict()
        countryiso = None
        for key in row:
            hxltag = hxlrow[key]
            if hxltag != '':
                value = row[key]
                newrow[hxlrow[key]] = value
                if hxltag == '#country+code':
                    countryiso = value
                    if countryiso not in countryiso3s:
                        countryiso = None
                        break
                    campaigns_per_country[countryiso] = campaigns_per_country.get(countryiso, 0) + 1
                if hxltag == '#status+name':
                    value = value.lower()
                    if value != 'on track' and 'reinstated' not in value:
                        affected_campaigns_per_country[countryiso] = affected_campaigns_per_country.get(countryiso, 0) + 1
        if countryiso:
            outputs['json'].add_data_row(name, newrow)
    ratios = calculate_ratios(campaigns_per_country, affected_campaigns_per_country)
    hxltag = '#vaccination+num+ratio'
    logger.info('Processed vaccination campaigns')
    return [['Vaccination Ratio'], [hxltag]], [ratios], [(hxltag, datasetinfo['date'], datasetinfo['source'], datasetinfo['source_url'])]
