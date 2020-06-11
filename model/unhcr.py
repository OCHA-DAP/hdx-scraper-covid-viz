# -*- coding: utf-8 -*-
import inspect
from os.path import join

from model import today_str


def get_unhcr(configuration, countryiso3s, downloader, scraper=None):
    if scraper and scraper not in inspect.currentframe().f_code.co_name:
        return list(), list(), list()
    iso3tocode = downloader.download_tabular_key_value(join('config', 'UNHCR_geocode.csv'))
    base_url = configuration['unhcr_url']
    valuedicts = [dict(), dict()]
    for countryiso3 in countryiso3s:
        code = iso3tocode.get(countryiso3)
        if not code:
            continue
        r = downloader.download('%s%s' % (base_url, code))
        data = r.json()['data'][0]
        valuedicts[0][countryiso3] = data['individuals']
        valuedicts[1][countryiso3] = data['date']
    hxltags = ['#affected+refugees', '#affected+date+refugees']
    return [['TotalRefugees', 'TotalRefugeesDate'], hxltags], valuedicts, [[hxltag, today_str, 'UNHCR', configuration['unhcr_source_url']] for hxltag in hxltags]


