# -*- coding: utf-8 -*-
import logging

from model import get_percent, today_str

logger = logging.getLogger(__name__)


def get_fts(configuration, countryiso3s, downloader):
    requirements = dict()
    funding = dict()
    percentage = dict()
    global_max_year = 0
    global_plan_id = 0
    for country in countryiso3s:
        url = '%splan/country/%s' % (configuration['fts_url'], country)
        response = downloader.download(url)
        json = response.json()
        max_year = 0
        plan_id = 0
        for plan in json['data']:
            if plan['categories'][0]['name'].lower() != 'humanitarian response plan':
                continue
            year = int(plan['years'][0]['year'])
            name = plan['planVersion']['name'].lower()
            if 'covid' in name and 'global' in name:
                if year >= global_max_year:
                    global_max_year = year
                    global_plan_id = plan['id']
            else:
                if year >= max_year:
                    max_year = year
                    plan_id = plan['id']

        if plan_id == 0:
            raise ValueError('No HRP found for %s!' % country)

        url = '%sfts/flow?planid=%d&groupby=cluster' % (configuration['fts_url'], plan_id)
        response = downloader.download(url)
        json = response.json()
        data = json['data']

        req = 0
        for reqobj in data['requirements']['objects']:
            if 'COVID-19' in reqobj['tags']:
                req += reqobj['revisedRequirements']
        requirements[country] = req

        fund = 0
        fundingobjects = data['report3']['fundingTotals']['objects']
        if len(fundingobjects) == 0:
            funding[country] = None  # Not Yet Tracked
            percentage[country] = None
        else:
            for fundobj in fundingobjects[0]['objectsBreakdown']:
                if 'COVID-19' in fundobj['name']:
                    fund += fundobj['totalFunding']
            funding[country] = fund
            if fund == 0:
                percentage[country] = 0
            else:
                percentage[country] = get_percent(fund, req)

    if global_plan_id == 0:
        raise ValueError('No GHRP found!')
    logger.info('Processed FTS')
    hxltags = ['#value+funding+required+covid+usd', '#value+funding+total+covid+usd', '#value+funding+covid+pct']
    return [['RequiredCovidFunding', 'CovidFunding', 'CovidPercentFunded'],
            hxltags], \
           [requirements, funding, percentage], \
           [[hxltag, today_str, 'https://fts.unocha.org/appeals/952/summary'] for hxltag in hxltags]
