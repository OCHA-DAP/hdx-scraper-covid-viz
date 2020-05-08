# -*- coding: utf-8 -*-
from jsonpath_rw import parse

from model import RowParser


def get_fts(configuration, countries, downloader):
    requirements = dict()
    funding = dict()
    global_max_year = 0
    global_plan_id = 0
    for country in countries:
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
        requirements[country] = 0
        for reqobj in data['requirements']['objects']:
            if 'COVID-19' in reqobj['tags']:
                requirements[country] += reqobj['revisedRequirements']
        funding[country] = 0
        fundingobjects = data['report3']['fundingTotals']['objects']
        if len(fundingobjects) == 0:
            funding[country] = None  # Not Yet Tracked
        else:
            for fundobj in fundingobjects[0]['objectsBreakdown']:
                if 'COVID-19' in fundobj['name']:
                    funding[country] += fundobj['totalFunding']

    if global_plan_id == 0:
        raise ValueError('No GHRP found!')
    return [['Required', 'Funding'], ['#value+funding+required+usd', '#value+funding+funding+usd']], \
           [requirements, funding]
