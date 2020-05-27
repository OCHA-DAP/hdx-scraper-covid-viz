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
        fund = data['report3']['fundingTotals']['total']
        return 0, fund, 0, fund

    covid_ids = list()
    covidflag = True
    covidreq = 0
    allreq = 0
    for reqobj in data['requirements']['objects']:
        req = reqobj.get('revisedRequirements')
        if req:
            allreq += req
            tags = reqobj.get('tags')
            if tags and 'COVID-19' in tags:
                covidreq += req
                covid_ids.append(reqobj['id'])
    if len(covid_ids) == 0:
        logger.info('%s has no COVID component!' % plan_id)
        covidflag = False

    covidfund = 0
    allfund = 0
    fundingobjects = data['report3']['fundingTotals']['objects']
    if len(fundingobjects) != 0:
        for fundobj in fundingobjects[0]['singleFundingObjects']:
            fund_id = fundobj.get('id')
            fund = fundobj['totalFunding']
            allfund += fund
            if covidflag and fund_id and fund_id in covid_ids:
                covidfund += fund
        sharedfundingobjects = fundingobjects[0].get('sharedFundingObjects')
        if sharedfundingobjects:
            for fundobj in sharedfundingobjects:
                fund_ids = fundobj.get('id')
                fund = fundobj['totalFunding']
                allfund += fund
                if covidflag and fund_ids:
                    match = True
                    for fund_id in fund_ids:
                        if int(fund_id) not in covid_ids:
                            match = False
                            break
                    if match:
                        covidfund += fund
    if not covidflag:
        return allreq, allfund, None, None
    return allreq, allfund, covidreq, covidfund


def get_fts(configuration, countryiso3s, downloader, scraper=None):
    if scraper and scraper not in inspect.currentframe().f_code.co_name:
        return list(), list(), list(), list(), list(), list()
    requirements = [dict(), dict(), dict()]
    funding = [dict(), dict(), dict()]
    percentage = [dict(), dict(), dict()]

    base_url = configuration['fts_url']
    url = '%splan/year/%d' % (base_url, today.year)
    data = download_data(url, downloader)
    total_covidreq = 0
    total_covidfund = 0
    rows = list()
    for plan in data:
        plan_id = plan['id']
        emergencies = plan['emergencies']
        if len(emergencies) == 1 and emergencies[0]['id'] == 911:
            isghrp = True
        else:
            isghrp = False
        allreq, allfund, covidreq, covidfund = get_requirements_and_funding(base_url, plan_id, downloader, isghrp)
        name = plan['planVersion']['name']
        if covidreq or covidfund:
            rows.append([name, covidreq, covidfund])
            logger.info('%s: Requirements=%d, Funding=%d' % (name, covidreq, covidfund))
            if covidreq:
                total_covidreq += covidreq
            if covidfund:
                total_covidfund += covidfund
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
        if index == 0:
            if allreq:
                requirements[index][countryiso] = allreq
            else:
                requirements[index][countryiso] = None
            if allfund and allreq:
                funding[index][countryiso] = allfund
                percentage[index][countryiso] = get_percent(allfund, allreq)
        if covidreq:
            requirements[index + 1][countryiso] = covidreq
        else:
            requirements[index + 1][countryiso] = None
        if covidfund and covidreq:
            funding[index + 1][countryiso] = covidfund
            percentage[index + 1][countryiso] = get_percent(covidfund, covidreq)
    total_percent = get_percent(total_covidfund, total_covidreq)
    logger.info('Processed FTS')
    write_list_to_csv('ftscovid.csv', rows, ['Name', 'Requirements', 'Funding'])
    whxltags = ['#value+covid+funding+ghrp+required+usd', '#value+covid+funding+ghrp+total+usd', '#value+covid+funding+ghrp+pct']
    hxltags = ['#value+funding+hrp+required+usd', '#value+funding+hrp+total+usd', '#value+funding+hrp+pct',
               '#value+covid+funding+hrp+required+usd', '#value+covid+funding+hrp+total+usd', '#value+covid+funding+hrp+pct',
               '#value+covid+funding+other+required+usd', '#value+covid+funding+other+total+usd', '#value+covid+funding+other+pct']
    return [['RequiredHRPCovidFunding', 'GHRPCovidFunding', 'GHRPCovidPercentFunded'], whxltags], \
           [total_covidreq, total_covidfund, total_percent], \
           [[hxltag, today_str, 'OCHA', 'https://fts.unocha.org/appeals/952/summary'] for hxltag in whxltags], \
           [['RequiredHRPFunding', 'HRPFunding', 'HRPPercentFunded',
             'RequiredHRPCovidFunding', 'HRPCovidFunding', 'HRPCovidPercentFunded',
             'RequiredOtherCovidFunding', 'OtherCovidFunding', 'OtherCovidPercentFunded'], hxltags], \
           [requirements[0], funding[0], percentage[0], requirements[1], funding[1], percentage[1],
            requirements[2], funding[2], percentage[2]], \
           [[hxltag, today_str, 'OCHA', 'https://fts.unocha.org/appeals/952/summary'] for hxltag in hxltags]
