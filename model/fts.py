# -*- coding: utf-8 -*-
import inspect
import logging

from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import write_list_to_csv

from model import get_percent, today_str, today, get_date_from_dataset_date

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


def get_requirements_and_funding(v1_url, v2_url, plan_id, downloader):
    url = '%spublic/governingEntity?planId=%d&scopes=governingEntityVersion' % (v2_url, plan_id)
    data = download_data(url, downloader)
    covid_ids = set()
    for clusterobj in data:
        tags = clusterobj['governingEntityVersion'].get('tags')
        if tags and 'COVID-19' in tags:
            covid_ids.add(clusterobj['id'])
    if len(covid_ids) == 0:
        logger.info('%s has no COVID component!' % plan_id)
        return None, None

    url = '%sfts/flow?planid=%d&groupby=cluster' % (v1_url, plan_id)
    data = download_data(url, downloader)
    covidreq = 0
    for reqobj in data['requirements']['objects']:
        req = reqobj.get('revisedRequirements')
        if req:
            req_id = reqobj.get('id')
            if req_id and req_id in covid_ids:
                covidreq += req

    covidfund = 0
    fundingobjects = data['report3']['fundingTotals']['objects']
    if len(fundingobjects) != 0:
        singlefundingobjects = fundingobjects[0].get('singleFundingObjects')
        if singlefundingobjects:
            for fundobj in singlefundingobjects:
                fund_id = fundobj.get('id')
                fund = fundobj['totalFunding']
                if fund_id and fund_id in covid_ids:
                    covidfund += fund
        sharedfundingobjects = fundingobjects[0].get('sharedFundingObjects')
        if sharedfundingobjects:
            for fundobj in sharedfundingobjects:
                fund_ids = fundobj.get('id')
                fund = fundobj['totalFunding']
                if fund_ids:
                    match = True
                    for fund_id in fund_ids:
                        if int(fund_id) not in covid_ids:
                            match = False
                            break
                    if match:
                        covidfund += fund
    return covidreq, covidfund


def get_fts(configuration, countryiso3s, downloader, scraper=None):
    if scraper and scraper not in inspect.currentframe().f_code.co_name:
        return list(), list(), list(), list(), list(), list()
    requirements = [dict(), dict(), dict()]
    funding = [dict(), dict(), dict()]
    percentage = [dict(), dict(), dict()]

    v1_url = configuration['fts_v1_url']
    v2_url = configuration['fts_v2_url']

    url = '%sfts/flow/plan/overview/progress/%d' % (v2_url, today.year)
    data = download_data(url, downloader)
    total_covidreq = 0
    total_covidfund = 0

    rows = list()
    for plan in data['plans']:
        plan_id = plan['id']
        allreq = plan['requirements']['revisedRequirements']
        allfund = plan.get('funding')
        if allfund:
            allfund = allfund['totalFunding']
        if plan_id == 952:
            covidreq = allreq
            covidfund = allfund
        else:
            covidreq, covidfund = get_requirements_and_funding(v1_url, v2_url, plan_id, downloader)
        name = plan['name']
        if plan['planType']['includeTotals']:
            if covidreq or covidfund:
                rows.append([name, covidreq, covidfund])
                logger.info('%s: Requirements=%d, Funding=%d' % (name, covidreq, covidfund))
                if covidreq:
                    total_covidreq += covidreq
                if covidfund:
                    total_covidfund += covidfund
        countries = plan['countries']
        iso3s = set()
        for country in countries:
            countryiso = country['iso3']
            if countryiso:
                iso3s.add(countryiso)
        if len(iso3s) == 1:
            countryiso = iso3s.pop()
            if not countryiso or countryiso not in countryiso3s:
                continue
            plan_type = plan['planType']['name'].lower()
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
                percentage[index][countryiso] = get_percent(plan['funding']['progress'], 100)
        if covidreq:
            requirements[index + 1][countryiso] = covidreq
        else:
            requirements[index + 1][countryiso] = None
        if covidfund and covidreq:
            funding[index + 1][countryiso] = covidfund
            percentage[index + 1][countryiso] = get_percent(covidfund, covidreq)

    total_allreq = data['totals']['revisedRequirements']
    total_allfund = data['totals']['totalFunding']
    total_allpercent = get_percent(data['totals']['progress'], 100)
    total_covidpercent = get_percent(total_covidfund, total_covidreq)
    logger.info('Processed FTS')
    write_list_to_csv('ftscovid.csv', rows, ['Name', 'Requirements', 'Funding'])
    whxltags = ['#value+funding+required+usd', '#value+funding+total+usd', '#value+funding+pct',
                '#value+covid+funding+ghrp+required+usd', '#value+covid+funding+ghrp+total+usd', '#value+covid+funding+ghrp+pct']
    hxltags = ['#value+funding+hrp+required+usd', '#value+funding+hrp+total+usd', '#value+funding+hrp+pct',
               '#value+covid+funding+hrp+required+usd', '#value+covid+funding+hrp+total+usd', '#value+covid+funding+hrp+pct',
               '#value+funding+other+planname', '#value+funding+other+required+usd', '#value+funding+other+total+usd', '#value+funding+other+pct']
    total_allreq = {'global': total_allreq}
    total_allfund = {'global': total_allfund}
    total_allpercent = {'global': total_allpercent}
    total_covidreq = {'global': total_covidreq}
    total_covidfund = {'global': total_covidfund}
    total_covidpercent = {'global': total_covidpercent}
    return [['RequiredFunding', 'Funding', 'PercentFunded',
             'RequiredGHRPCovidFunding', 'GHRPCovidFunding', 'GHRPCovidPercentFunded'], whxltags], \
           [total_allreq, total_allfund, total_allpercent, total_covidreq, total_covidfund, total_covidpercent], \
           [[hxltag, today_str, 'OCHA', 'https://fts.unocha.org/appeals/952/summary'] for hxltag in whxltags], \
           [['RequiredHRPFunding', 'HRPFunding', 'HRPPercentFunded',
             'RequiredHRPCovidFunding', 'HRPCovidFunding', 'HRPCovidPercentFunded',
             'OtherPlans', 'RequiredOtherPlansFunding', 'OtherPlansFunding', 'OtherPlansPercentFunded'],
             hxltags], \
           [requirements[0], funding[0], percentage[0], requirements[1], funding[1], percentage[1],
            requirements[2], funding[2], percentage[2]], \
           [[hxltag, today_str, 'OCHA', configuration['fts_source_url']] for hxltag in hxltags]
