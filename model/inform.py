# -*- coding: utf-8 -*-
import inspect
import logging

from hdx.utilities.dateparse import default_date, parse_date
from hdx.utilities.downloader import Download

logger = logging.getLogger(__name__)


def get_inform(configuration, today, countryiso3s, scrapers=None):
    name = inspect.currentframe().f_code.co_name
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list()
    inform_configuration = configuration['inform']
    url = inform_configuration['url'] % today.strftime('%b%Y')
    input_cols = inform_configuration['input_cols']
    page = 1
    countries_index = {'individual': dict(), 'aggregated': dict()}
    iso3s_present = set()
    with Download(rate_limit={'calls': 1, 'period': 0.1}, auth='') as downloader:
        while True:
            r = downloader.download(f'{url}{page}')
            json = r.json()
            for result in json['results']:
                countryiso3 = result['iso3']
                if len(countryiso3) != 1:
                    continue
                countryiso3 = countryiso3[0]
                if countryiso3 not in countryiso3s:
                    continue
                iso3s_present.add(countryiso3)
                individual_or_aggregated = result['individual_or_aggregated']
                countries_index_individual_or_aggregated = countries_index[individual_or_aggregated]
                country_index = countries_index_individual_or_aggregated.get(countryiso3, dict())
                country_index['date'] = result['Last updated']
                for input_col in input_cols:
                    country_index[input_col] = result[input_col]
                countries_index_individual_or_aggregated[countryiso3] = country_index
            if json['next'] is None:
                break
            page += 1
    individual_index = country_index['individual']
    aggregated_index = country_index['aggregated']
    valuedicts = [dict() for _ in input_cols]
    date = default_date
    for countryiso3 in iso3s_present:
        if countryiso3 in aggregated_index:
            country_index = aggregated_index[countryiso3]
        else:
            country_index = individual_index[countryiso3]
        newdate = parse_date(country_index['date'])
        if newdate > date:
            date = newdate
        for i, input_col in enumerate(input_cols):
            valuedicts[i][countryiso3] = country_index[input_col]
    logger.info('Processed INFORM')
    source_date = date.date().isoformat()
    output_cols = inform_configuration['output_cols']
    hxltags = inform_configuration['output_hxltags']
    return [output_cols, hxltags], valuedicts, [(hxltag, source_date, 'INFORM', 'https://data.humdata.org/dataset/covid-19-data-visual-inputs') for hxltag in hxltags]