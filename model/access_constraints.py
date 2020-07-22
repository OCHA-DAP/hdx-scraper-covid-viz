# -*- coding: utf-8 -*-
import inspect
import logging

from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add

from model import today_str
from model.readers import read_tabular, read_hdx

logger = logging.getLogger(__name__)


def process_range(ranges, score):
    for numrange in ranges:
        if '-' in numrange:
            start, end = numrange.split('-')
            if int(start) <= score <= int(end):
                return ranges[numrange]
        elif '+' in numrange:
            if score >= int(numrange[:-1]):
                return ranges[numrange]
        else:
            raise ValueError('Invalid range %s!' % numrange)
    raise ValueError('Score %s not found in ranges!' % score)


def get_access(configuration, countryiso3s, downloader, scraper=None):
    if scraper and scraper not in inspect.currentframe().f_code.co_name:
        return list(), list(), list()
    access_configuration = configuration['access_constraints']
    ranking_url = access_configuration['ranking_url']
    headers, rows = read_tabular(downloader, {'url': ranking_url, 'headers': 1, 'format': 'csv'})
    sheets = access_configuration['sheets']
    constraint_rankings = {x: dict() for x in sheets}
    for row in rows:
        countryiso = row['iso3']
        for sheet in sheets:
            type_ranking = constraint_rankings.get(sheet, dict())
            for i in range(1, 4):
                constraint = row['%s_%d' % (sheet, i)]
                dict_of_lists_add(type_ranking, countryiso, constraint)
            constraint_rankings[sheet] = type_ranking
    data = dict()
    datasetinfo = {'dataset': access_configuration['dataset'], 'headers': 1, 'format': 'xlsx'}
    for sheet, sheetinfo in sheets.items():
        datasetinfo['sheet'] = sheetinfo['sheetname']
        headers, rows = read_hdx(downloader, datasetinfo)
        datasheet = data.get(sheet, dict())
        for row in rows:
            countryiso = Country.get_iso3_country_code(row[sheetinfo['isocol']])
            if countryiso not in countryiso3s:
                continue
            countrydata = datasheet.get(countryiso, dict())
            score = countrydata.get('score', 0)
            newscore = row[sheetinfo['scorecol']]
            textcol = sheetinfo.get('textcol')
            if textcol:
                text = row[textcol]
                dict_of_lists_add(countrydata, 'text', (newscore, text))
                weights = sheetinfo.get('weights')
                if weights:
                    weight = weights.get(text)
                    if weight:
                        newscore *= weight
            score += newscore
            countrydata['score'] = score
            datasheet[countryiso] = countrydata
        data[sheet] = datasheet
    valuedicts = [dict() for _ in range(5)]
    severityscore = valuedicts[0]
    for i, sheet in enumerate(data):
        datasheet = data[sheet]
        for countryiso in datasheet:
            countrydata = datasheet[countryiso]
            ranked = sorted(countrydata['text'], reverse=True)
            top_value = ranked[0][0]
            texts = list()
            for value, text in countrydata['text']:
                if value == top_value:
                    if text in constraint_rankings[sheet][countryiso]:
                        texts.append(text)
            valuedicts[i+2][countryiso] = '|'.join(texts)
            if sheet != 'impact':
                score = severityscore.get(countryiso, 0)
                score += countrydata['score']
                severityscore[countryiso] = score
    ranges = access_configuration['category']
    severitycategory = valuedicts[1]
    for countryiso in severityscore:
        score = severityscore.get(countryiso)
        if score is None:
            severitycategory[countryiso] = None
            continue
        severitycategory[countryiso] = process_range(ranges, score)
    logger.info('Processed access')
    hxltags = ['#severity+access+num+score', '#severity+access+category+num', '#access+constraints+into', '#access+constraints+within', '#access+impact']
    return [['Access Severity Score', 'Access Severity Category', 'Access Constraints Into', 'Access Constraints Within', 'Access Impact'], hxltags], valuedicts, [(hxltag, datasetinfo['date'], datasetinfo['source'], datasetinfo['source_url']) for hxltag in hxltags]



