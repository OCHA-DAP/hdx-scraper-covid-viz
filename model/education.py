# -*- coding: utf-8 -*-
import logging

from hdx.scraper.readers import read
from hdx.utilities.dateparse import default_date, parse_date
from hdx.utilities.text import get_fraction_str, get_numeric_if_possible

logger = logging.getLogger(__name__)


def get_education(configuration, countryiso3s, regionlookup, downloader, scrapers=None):
    name = 'education'
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list(), list(), list(), list()
    datasetinfo = configuration[name]
    closures_headers, closures_iterator = read(downloader, datasetinfo)
    closures = dict()
    curdate = default_date
    for row in closures_iterator:
        countryiso = row['ISO']
        if not countryiso or countryiso not in countryiso3s:
            continue
        date = parse_date(row['Date'])
        if date < curdate:
            continue
        if date > curdate:
            curdate = date
            closures = dict()
        closures[countryiso] = row['Status']
    fully_closed = list()
    for countryiso, closure in closures.items():
        if closure.lower() == 'closed due to covid-19':
            fully_closed.append(countryiso)
    datasetinfo['url'] = datasetinfo['edu_url']
    learners_headers, learners_iterator = read(downloader, datasetinfo)
    hxlrow = next(learners_iterator)
    learners_pre12 = dict()
    learners_3 = dict()
    curdate = default_date
    for row in learners_iterator:
        newrow = dict()
        for key in row:
            newrow[hxlrow[key]] = row[key]
        countryiso = newrow['#country+code']
        if not countryiso or countryiso not in countryiso3s:
            continue
        date = parse_date(newrow['#date'])
        if date < curdate:
            continue
        if date > curdate:
            curdate = date
            learners_pre12[countryiso] = dict()
            learners_3[countryiso] = dict()
        learners_pre12[countryiso] = get_numeric_if_possible(newrow['#population+learners+pre_primary_to_secondary'])
        learners_3[countryiso] = get_numeric_if_possible(newrow['#population+learners+tertiary'])
    affected_learners_total = dict()
    learners_total = dict()
    closed_countries = dict()
    for countryiso, l_p12 in learners_pre12.items():
        l_3 = learners_3[countryiso]
        l_t = 0
        if l_p12:
            l_t += l_p12
        if l_3:
            l_t += l_3
        for region in regionlookup.iso3_to_region_and_hrp[countryiso]:
            learners_total[region] = learners_total.get(region, 0) + l_t
            if countryiso in fully_closed:
                affected_learners_total[region] = affected_learners_total.get(region, 0) + l_t
                closed_countries[region] = closed_countries.get(region, 0) + 1
    percentage_affected_learners = dict()
    for region, affected_learners in affected_learners_total.items():
        percentage_affected_learners[region] = get_fraction_str(affected_learners, learners_total[region])
    logger.info('Processed education')
    grheaders = ['No. affected learners', 'Percentage affected learners', 'No. closed countries']
    grhxltags = ['#affected+learners+num', '#affected+learners+pct', '#status+country+closed']
    headers = ['School Closure', 'No. pre-primary to upper-secondary learners', 'No. tertiary learners']
    hxltags = ['#impact+type', '#population+learners+pre_primary_to_secondary', '#population+learners+tertiary']
    return [grheaders, grhxltags], [affected_learners_total, percentage_affected_learners, closed_countries], \
           [(hxltag, datasetinfo['date'], datasetinfo['source'], datasetinfo['source_url']) for hxltag in hxltags], \
           [headers, hxltags], [closures, learners_pre12, learners_3], \
           [(hxltag, datasetinfo['date'], datasetinfo['source'], datasetinfo['source_url']) for hxltag in hxltags]
