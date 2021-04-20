# -*- coding: utf-8 -*-
import inspect
import logging

from hdx.scraper.readers import read
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import get_numeric_if_possible

logger = logging.getLogger(__name__)


def get_covax_deliveries(configuration, today, countryiso3s, downloader, scrapers=None):
    name = 'covax_deliveries'
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list()
    datasetinfo = configuration[name]
    headers, iterator = read(downloader, datasetinfo, today=today)
    hxlrow = next(iterator)
    pipeline = dict()
    vaccine = dict()
    funder = dict()
    doses = dict()
    for row in iterator:
        newrow = dict()
        for key in row:
            newrow[hxlrow[key]] = row[key]
        countryiso = newrow['#country+code']
        if not countryiso or countryiso not in countryiso3s:
            continue
        dict_of_lists_add(pipeline, countryiso, newrow['#meta+vaccine+pipeline'])
        dict_of_lists_add(vaccine, countryiso, newrow['#meta+vaccine+producer'])
        dict_of_lists_add(funder, countryiso, newrow['#meta+vaccine+funder'])
        dict_of_lists_add(doses, countryiso, str(get_numeric_if_possible(newrow['#capacity+vaccine+doses'])))
    for countryiso in pipeline:
        pipeline[countryiso] = '|'.join(pipeline[countryiso])
        vaccine[countryiso] = '|'.join(vaccine[countryiso])
        funder[countryiso] = '|'.join(funder[countryiso])
        doses[countryiso] = '|'.join(doses[countryiso])
    logger.info('Processed covax deliveries')
    hxltags = ['#meta+vaccine+pipeline', '#meta+vaccine+producer', '#meta+vaccine+funder', '#capacity+vaccine+doses']
    return [['Pipeline', 'Vaccine', 'Funder', 'Doses'], hxltags], \
           [pipeline, vaccine, funder, doses], [(hxltag, datasetinfo['date'], datasetinfo['source'], datasetinfo['source_url']) for hxltag in hxltags]
