# -*- coding: utf-8 -*-
import logging

from hdx.utilities.dictandlist import dict_of_lists_add

from model import number_format, calculate_ratios
from model.readers import read_hdx

logger = logging.getLogger(__name__)


def add_vaccination_campaigns(configuration, countryiso3s, downloader, json, scraper):
    name = 'vaccination_campaigns'
    if scraper and scraper != name:
        return list(), list(), list()
    datasetinfo = configuration[name]
    headers, iterator = read_hdx(downloader, datasetinfo)
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
                    if value != 'On track':
                        affected_campaigns_per_country[countryiso] = affected_campaigns_per_country.get(countryiso, 0) + 1
        if countryiso:
            dict_of_lists_add(json, '%s_data' % name, newrow)
    ratios = calculate_ratios(campaigns_per_country, affected_campaigns_per_country)
    hxltag = '#vaccination+num+ratio'
    logger.info('Processed vaccination campaigns')
    return [['Vaccination Ratio'], [hxltag]], [ratios], [[hxltag, datasetinfo['date'], datasetinfo['source'], datasetinfo['source_url']]]
