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
    affected_campaigns_per_country2 = dict()
    for row in iterator:
        newrow = dict()
        countryiso = None
        status = None
        for key in row:
            hxltag = hxlrow[key]
            if hxltag == '':
                continue
            value = row[key]
            newrow[hxlrow[key]] = value
            if hxltag == '#country+code':
                countryiso = value
            elif hxltag == '#status+name':
                status = value.lower()
        if not countryiso or countryiso not in countryiso3s:
            continue
        if not status or status == 'completed as planned':
            continue
        outputs['json'].add_data_row(name, newrow)
        campaigns_per_country[countryiso] = campaigns_per_country.get(countryiso, 0) + 1
        if status != 'on track' and 'reinstated' not in status:
            affected_campaigns_per_country2[countryiso] = affected_campaigns_per_country2.get(countryiso, 0) + 1
        if status in ('postponed covid', 'cancelled'):
            affected_campaigns_per_country[countryiso] = affected_campaigns_per_country.get(countryiso, 0) + 1
    for countryiso in campaigns_per_country:
        if countryiso not in affected_campaigns_per_country:
            affected_campaigns_per_country[countryiso] = 0
    ratios = calculate_ratios(campaigns_per_country, affected_campaigns_per_country2)
    hxltags = ['#vaccination+postponed+num', '#vaccination+num+ratio']
    logger.info('Processed vaccination campaigns')
    return [['Vaccinations Postponed', 'Vaccination Ratio'], hxltags], [affected_campaigns_per_country, ratios], [(hxltag, datasetinfo['date'], datasetinfo['source'], datasetinfo['source_url']) for hxltag in hxltags]
