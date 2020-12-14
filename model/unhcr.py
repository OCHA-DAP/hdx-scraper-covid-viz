# -*- coding: utf-8 -*-
import inspect
import logging

from os.path import join

logger = logging.getLogger(__name__)


def get_unhcr(configuration, today_str, countryiso3s, downloader, scrapers=None):
    name = inspect.currentframe().f_code.co_name
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list()
    iso3tocode = downloader.download_tabular_key_value(join('config', 'UNHCR_geocode.csv'))
    unhcr_configuration = configuration['unhcr']
    base_url = unhcr_configuration['url']
    valuedicts = [dict(), dict()]
    for countryiso3 in countryiso3s:
        code = iso3tocode.get(countryiso3)
        if not code:
            continue
        r = downloader.download('%s%s' % (base_url, code))
        data = r.json()['data'][0]
        valuedicts[0][countryiso3] = data['individuals']
        valuedicts[1][countryiso3] = data['date']
    logger.info('Processed UNHCR')
    hxltags = ['#affected+refugees', '#affected+date+refugees']
    return [['TotalRefugees', 'TotalRefugeesDate'], hxltags], valuedicts, [(hxltag, today_str, 'UNHCR', unhcr_configuration['source_url']) for hxltag in hxltags]
