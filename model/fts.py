# -*- coding: utf-8 -*-
import inspect
import logging

from hdx.utilities.dictandlist import write_list_to_csv

from model import get_percent, today_str, today

logger = logging.getLogger(__name__)


class FTSException(Exception):
    pass


def download(url, downloader):
    r = downloader.download(url)
    json = r.json()
    status = json['status']
    if status != 'ok':
        raise FTSException('%s gives status %s' % (url, status))
    return json


def download_data(url, downloader):
    return download(url, downloader)['data']


def get_requirements_and_funding(base_url, plan_id, downloader, isghrp):
    url = '%sfts/flow?planid=%d&groupby=cluster' % (base_url, plan_id)
    data = download_data(url, downloader)
    if isghrp:
        return 0, data['report3']['fundingTotals']['total']

    covid_ids = list()
    req = 0
    for reqobj in data['requirements']['objects']:
        tags = reqobj.get('tags')
        if tags and 'COVID-19' in tags:
            req += reqobj['revisedRequirements']
            covid_ids.append(reqobj['id'])
    if len(covid_ids) == 0:
        logger.info('%s has no COVID component!' % plan_id)
        return None, None

    fund = 0
    fundingobjects = data['report3']['fundingTotals']['objects']
    if len(fundingobjects) != 0:
        for fundobj in fundingobjects[0]['singleFundingObjects']:
            fund_id = fundobj.get('id')
            if fund_id and fund_id in covid_ids:
                fund += fundobj['totalFunding']
        sharedfundingobjects = fundingobjects[0].get('sharedFundingObjects')
        if sharedfundingobjects:
            for fundobj in sharedfundingobjects:
                fund_ids = fundobj.get('id')
                if fund_ids:
                    match = True
                    for fund_id in fund_ids:
                        if int(fund_id) not in covid_ids:
                            match = False
                            break
                    if match:
                        fund += fundobj['totalFunding']
    return req, fund


def get_fts(configuration, countryiso3s, downloader, scraper=None):
    if scraper and scraper not in inspect.currentframe().f_code.co_name:
        return list(), list(), list()
    requirements = [dict(), dict()]
    funding = [dict(), dict()]
    percentage = [dict(), dict()]

    base_url = configuration['fts_url']
    url = '%splan/year/%d' % (base_url, today.year)
    data = download_data(url, downloader)
    total_req = 0
    total_fund = 0
    rows = list()
    for plan in data:
        plan_id = plan['id']
        emergencies = plan['emergencies']
        if len(emergencies) == 1 and emergencies[0]['id'] == 911:
            isghrp = True
        else:
            isghrp = False
        req, fund = get_requirements_and_funding(base_url, plan_id, downloader, isghrp)
        if not req and not fund:
            continue
        name = plan['planVersion']['name']
        rows.append([name, req, fund])
        logger.info('%s: Requirements=%d, Funding=%d' % (name, req, fund))
        if req:
            total_req += req
        if fund:
            total_fund += fund
        locations = plan['locations']
        iso3s = set()
        for location in locations:
            countryiso = location['iso3']
            if countryiso:
                iso3s.add(countryiso)
        if len(iso3s) == 1:
            countryiso = iso3s.pop()
            if not countryiso or countryiso not in countryiso3s:
                continue
            plan_type = plan['categories'][0]['name'].lower()
            if plan_type == 'humanitarian response plan':
                index = 0
            else:
                index = 1
        else:
            continue
        if req == 0:
            requirements[index][countryiso] = None
        else:
            requirements[index][countryiso] = req
        if fund != 0 and req != 0:
            funding[index][countryiso] = fund
            percentage[index][countryiso] = get_percent(fund, req)
    total_percent = get_percent(total_fund, total_req)
    logger.info('Processed FTS')
    write_list_to_csv('ftscovid.csv', rows, ['Name', 'Requirements', 'Funding'])
    whxltags = ['#value+covid+funding+ghrp+required+usd', '#value+covid+funding+ghrp+total+usd', '#value+covid+funding+ghrp+pct']
    hxltags = ['#value+covid+funding+hrp+required+usd', '#value+covid+funding+hrp+total+usd', '#value+covid+funding+hrp+pct',
               '#value+covid+funding+other+required+usd', '#value+covid+funding+other+total+usd', '#value+covid+funding+other+pct']
    return [['RequiredHRPCovidFunding', 'GHRPCovidFunding', 'GHRPCovidPercentFunded'], whxltags], \
           [total_req, total_fund, total_percent], \
           [[hxltag, today_str, 'OCHA', 'https://fts.unocha.org/appeals/952/summary'] for hxltag in whxltags], \
           [['RequiredHRPCovidFunding', 'HRPCovidFunding', 'HRPCovidPercentFunded',
             'RequiredOtherCovidFunding', 'OtherCovidFunding', 'OtherCovidPercentFunded'], hxltags], \
           [requirements[0], funding[0], percentage[0], requirements[1], funding[1], percentage[1]], \
           [[hxltag, today_str, 'OCHA', 'https://fts.unocha.org/appeals/952/summary'] for hxltag in hxltags]
