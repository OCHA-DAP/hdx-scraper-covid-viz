# -*- coding: utf-8 -*-
import inspect
import logging

from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add

from model import today_str, get_percent
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
        return list(), list(), list(), list(), list(), list()
    access_configuration = configuration['access_constraints']
    ranking_url = access_configuration['ranking_url']
    headers, rows = read_tabular(downloader, {'url': ranking_url, 'headers': 1, 'format': 'csv'})
    sheets = access_configuration['sheets']
    constraint_rankings = {x: dict() for x in sheets}
    nocountries = 0
    for row in rows:
        countryiso = row['iso3']
        nocountries += 1
        for sheet in sheets:
            if '%s_1' % sheet not in row:
                continue
            type_ranking = constraint_rankings.get(sheet, dict())
            for i in range(1, 4):
                constraint = row['%s_%d' % (sheet, i)]
                dict_of_lists_add(type_ranking, countryiso, constraint)
            constraint_rankings[sheet] = type_ranking
    data = dict()
    top3counts = dict()
    datasetinfo = {'dataset': access_configuration['dataset'], 'headers': 1, 'format': 'xlsx'}
    for sheet, sheetinfo in sheets.items():
        datasetinfo['sheet'] = sheetinfo['sheetname']
        headers, rows = read_hdx(downloader, datasetinfo)
        datasheet = data.get(sheet, dict())
        top3countssheet = top3counts.get(sheet, dict())
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
                if sheet == 'impact':
                    if newscore != 0:
                        top3countssheet[text] = top3countssheet.get(text, 0) + 1
                else:
                    if newscore == 3:
                        top3countssheet[text] = top3countssheet.get(text, 0) + 1
                weights = sheetinfo.get('weights')
                if weights:
                    weight = weights.get(text)
                    if weight:
                        newscore *= weight
                score += newscore
            else:
                dict_of_lists_add(countrydata, 'text', (newscore, newscore))
                if newscore == 'yes':
                    top3countssheet[sheet] = top3countssheet.get(sheet, 0) + 1
                score = newscore
            countrydata['score'] = score
            datasheet[countryiso] = countrydata
        data[sheet] = datasheet
        top3counts[sheet] = top3countssheet
    gvaluedicts = [dict() for _ in range(7)]
    for i, (sheet, top3countssheet) in enumerate(top3counts.items()):
        sortedcounts = sorted(top3countssheet, key=top3countssheet.get, reverse=True)
        texts = list()
        pcts = list()
        for text in sortedcounts[:3]:
            texts.append(text)
            pcts.append(get_percent(top3countssheet[text], nocountries))
        if sheet == 'mitigation':
            gvaluedicts[i * 2]['global'] = pcts[0]
        else:
            gvaluedicts[i * 2]['global'] = '|'.join(texts)
            gvaluedicts[i * 2 + 1]['global'] = '|'.join(pcts)
    valuedicts = [dict() for _ in range(6)]
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
                    if sheet == 'mitigation' or text in constraint_rankings[sheet][countryiso]:
                        texts.append(text)
            valuedicts[i+2][countryiso] = '|'.join(texts)
            if 'constraints' in sheet:
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
    gheaders = ['Access Constraints Into', 'Access Constraints Into Pct', 'Access Constraints Within', 'Access Constraints Within Pct', 'Access Impact', 'Access Impact Pct', 'Mitigation Pct']
    headers = ['Access Severity Score', 'Access Severity Category', 'Access Constraints Into', 'Access Constraints Within', 'Access Impact', 'Mitigation']
    ghxltags = ['#access+constraints+into+desc', '#access+constraints+into+pct', '#access+constraints+within+desc', '#access+constraints+within+pct', '#access+impact+desc', '#access+impact+pct', '#access+mitigation+pct']
    hxltags = ['#severity+access+num+score', '#severity+access+category+num', '#access+constraints+into+desc', '#access+constraints+within+desc', '#access+impact+desc', '#access+mitigation+desc']
    return [gheaders, ghxltags], gvaluedicts, \
           [(hxltag, datasetinfo['date'], datasetinfo['source'], datasetinfo['source_url']) for hxltag in hxltags], \
           [headers, hxltags], valuedicts, \
           [(hxltag, datasetinfo['date'], datasetinfo['source'], datasetinfo['source_url']) for hxltag in hxltags]



