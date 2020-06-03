import inspect
import logging
import time
from datetime import timedelta
from os.path import exists, join, getctime

from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import get_temp_dir

from model import today, get_date_from_timestamp
from model.readers import read_tabular

logger = logging.getLogger(__name__)


def get_fx(timeseries, configuration, countryiso3s, downloader, scraper=None):
    if scraper and scraper not in inspect.currentframe().f_code.co_name:
        return list()

    url = configuration['country_currency_url']
    _, data = read_tabular(downloader, {'url': url, 'sheet': 'Active', 'headers': 4, 'format': 'xls'})
    mapping = dict()
    for row in data:
        countryname = row['ENTITY']
        try:
            countryiso3, _ = Country.get_iso3_country_code_fuzzy(countryname)
        except:
            continue
        mapping[countryiso3] = row['Alphabetic Code']
    base_url = configuration['alphavantage_url']
    max_date = parse_date('1900-01-01')
    sources = list()
    temp_dir = get_temp_dir('fx')
    for countryiso3 in countryiso3s:
        currency = mapping[countryiso3]
        filename = '%s.csv' % currency
        path = join(temp_dir, filename)
        if not exists(path) or (today - get_date_from_timestamp(getctime(path))) > timedelta(hours=12):
            url = base_url % currency
            downloader.download_file(url, temp_dir, filename)
            time.sleep(20)
        _, data = read_tabular(downloader, {'url': path, 'headers': 1, 'format': 'csv'})
        norows = 0
        for row in data:
            date = row.get('timestamp')
            if not date:
                continue
            norows += 1
            val = row['close']
            timeseries.append([countryiso3, date, currency, val])
            date = parse_date(date)
            if date > max_date:
                max_date = date
        sources.append([currency, max_date.strftime('%Y-%m-%d'), 'Alpha Vantage', 'https://www.alphavantage.co/'])
        if norows == 0:
            logger.error('Problem with %s: %s' % (countryiso3, currency))
        else:
            logger.info('Processed %s: %s' % (countryiso3, currency))
    return sources
