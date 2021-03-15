# -*- coding: utf-8 -*-
import inspect
import logging

from dateutil.relativedelta import relativedelta
from hdx.scraper.readers import read_hdx_metadata
from hdx.utilities.dateparse import default_date, parse_date
from hdx.utilities.downloader import Download

logger = logging.getLogger(__name__)


def get_columns_by_date(date, base_url, countryiso3s, input_cols, downloader):
    url = base_url % date.strftime('%b%Y')
    countries_index = {'Individual': dict(), 'Aggregated': dict()}
    iso3s_present = set()
    while url:
        r = downloader.download(url)
        json = r.json()
        for result in json['results']:
            countryiso3 = result['iso3']
            if len(countryiso3) != 1:
                continue
            countryiso3 = countryiso3[0]
            if countryiso3 not in countryiso3s:
                continue
            if result['country_level'] != 'Yes':
                continue
            iso3s_present.add(countryiso3)
            individual_or_aggregated = result['individual_aggregated']
            countries_index_individual_or_aggregated = countries_index[individual_or_aggregated]
            country_index = countries_index_individual_or_aggregated.get(countryiso3, dict())
            country_index['date'] = result['Last updated']
            type_of_crisis = result['type_of_crisis']
            for input_col in input_cols:
                country_index[input_col] = (result[input_col], type_of_crisis)
            countries_index_individual_or_aggregated[countryiso3] = country_index
        url = json['next']
    individual_index = countries_index['Individual']
    aggregated_index = countries_index['Aggregated']
    valuedicts = [dict() for _ in input_cols]
    crisis_type = dict()
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
            val, type_of_crisis = country_index[input_col]
            if val is not None:
                valuedicts[i][countryiso3] = val
                crisis_type[countryiso3] = type_of_crisis
    valuedicts.append(crisis_type)
    return valuedicts, date


def get_inform(configuration, today, countryiso3s, other_auths, scrapers=None):
    name = inspect.currentframe().f_code.co_name
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list()
    inform_configuration = configuration['inform']
    read_hdx_metadata(inform_configuration)
    input_cols = inform_configuration['input_cols']
    trend_input_col = inform_configuration['trend_input_col']
    base_url = inform_configuration['url']
    with Download(rate_limit={'calls': 1, 'period': 0.1}, headers={'Authorization': other_auths['inform']}) as downloader:
        valuedictsfortoday, date = get_columns_by_date(today, base_url, countryiso3s, input_cols, downloader)
        crisis_types = [valuedictsfortoday.pop()]
        severity_indices = [valuedictsfortoday[0]]
        input_col = [trend_input_col]
        for i in range(1, 6, 1):
            prevdate = today - relativedelta(months=i)
            valuedictsfordate, _ = get_columns_by_date(prevdate, base_url, countryiso3s, input_col, downloader)
            crisis_types.append(valuedictsfordate[1])
            severity_indices.append(valuedictsfordate[0])
    trend_valuedict = dict()
    valuedictsfortoday.append(trend_valuedict)
    for countryiso in severity_indices[0]:
        trend = None
        crisis_type = crisis_types[0][countryiso]
        for other_type in crisis_types[1:]:
            if crisis_type != other_type[countryiso]:
                trend = '-'
                break
        if trend is None:
            avg = round((severity_indices[0][countryiso] + severity_indices[1][countryiso] + severity_indices[2][countryiso]) / 3, 1)
            prevavg = round((severity_indices[3][countryiso] + severity_indices[4][countryiso] + severity_indices[5][countryiso]) / 3, 1)
            if avg == prevavg:
                trend = 'stable'
            elif avg < prevavg:
                trend = 'decreasing'
            else:
                trend = 'increasing'
        trend_valuedict[countryiso] = trend
    logger.info('Processed INFORM')
    source_date = date.date().isoformat()
    output_cols = inform_configuration['output_cols'] + [inform_configuration['trend_output_col']]
    hxltags = inform_configuration['output_hxltags'] + [inform_configuration['trend_hxltag']]
    return [output_cols, hxltags], valuedictsfortoday, [(hxltag, source_date, inform_configuration['source'], inform_configuration['source_url']) for hxltag in hxltags]