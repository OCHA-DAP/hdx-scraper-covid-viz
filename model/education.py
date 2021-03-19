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
    affected_learners = dict()
    all_learners = dict()
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
        l_p12 = get_numeric_if_possible(newrow['#population+learners+pre_primary_to_secondary'])
        learners_pre12[countryiso] = l_p12
        l_3 = get_numeric_if_possible(newrow['#population+learners+tertiary'])
        learners_3[countryiso] = l_3
        no_learners = None
        if l_p12:
            no_learners = l_p12
            if l_3:
                no_learners += l_3
        elif l_3:
            no_learners = l_3
        if no_learners is not None:
            all_learners[countryiso] = no_learners
            if countryiso in fully_closed:
                affected_learners[countryiso] = no_learners
    affected_learners_total = dict()
    learners_total = dict()
    closed_countries = dict()
    for countryiso in closures:
        country_learners = all_learners.get(countryiso)
        country_affected_learners = affected_learners.get(countryiso)
        for region in regionlookup.iso3_to_region_and_hrp[countryiso]:
            if country_learners is not None:
                learners_total[region] = learners_total.get(region, 0) + country_learners
            if country_affected_learners is not None:
                affected_learners_total[region] = affected_learners_total.get(region, 0) + country_affected_learners
                closed_countries[region] = closed_countries.get(region, 0) + 1
    percentage_affected_learners = dict()
    for region, no_learners in affected_learners_total.items():
        percentage_affected_learners[region] = get_fraction_str(no_learners, learners_total[region])
    logger.info('Processed education')
    grheaders = ['No. affected learners', 'Percentage affected learners', 'No. closed countries']
    grhxltags = ['#affected+learners', '#affected+learners+pct', '#status+country+closed']
    headers = ['School Closure', 'No. pre-primary to upper-secondary learners', 'No. tertiary learners', 'No. affected learners']
    hxltags = ['#impact+type', '#population+learners+pre_primary_to_secondary', '#population+learners+tertiary', '#affected+learners']
    return [grheaders, grhxltags], [affected_learners_total, percentage_affected_learners, closed_countries], \
           [(hxltag, datasetinfo['date'], datasetinfo['source'], datasetinfo['source_url']) for hxltag in hxltags], \
           [headers, hxltags], [closures, learners_pre12, learners_3, affected_learners], \
           [(hxltag, datasetinfo['date'], datasetinfo['source'], datasetinfo['source_url']) for hxltag in hxltags]
