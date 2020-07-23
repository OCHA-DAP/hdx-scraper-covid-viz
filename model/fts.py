# -*- coding: utf-8 -*-
import inspect
import logging
import re

from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import write_list_to_csv, dict_of_lists_add
from hdx.utilities.text import multiple_replace

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


def get_requirements_and_funding_location(v1_url, plan_id, countryid_iso3mapping, countryiso3s, downloader):
    url = '%sfts/flow?planid=%d&groupby=location' % (v1_url, plan_id)
    data = download_data(url, downloader)
    allreqs, allfunds = dict(), dict()
    for reqobj in data['requirements']['objects']:
        countryid = reqobj.get('id')
        if not countryid:
            continue
        countryiso = countryid_iso3mapping.get(countryid)
        if not countryiso:
            continue
        if countryiso not in countryiso3s:
            continue
        req = reqobj.get('revisedRequirements')
        if req:
            allreqs[countryiso] = req

    fundingobjects = data['report3']['fundingTotals']['objects']
    if len(fundingobjects) != 0:
        singlefundingobjects = fundingobjects[0].get('singleFundingObjects')
        if singlefundingobjects:
            for fundobj in singlefundingobjects:
                countryid = fundobj.get('id')
                if not countryid:
                    continue
                countryiso = countryid_iso3mapping.get(countryid)
                if not countryiso:
                    continue
                if countryiso not in countryiso3s:
                    continue
                allfunds[countryiso] = fundobj['totalFunding']
    return allreqs, allfunds


def map_planname(origname):
    name = None
    if 'Refugee' in origname:
        words = origname.split(' ')
        try:
            index = words.index('Regional')
            name = ' '.join(words[:index+1])
        except ValueError:
            try:
                index = words.index('from')
                newwords = list()
                for word in words[index+1:]:
                    if '(' in word:
                        break
                    newwords.append(word)
                name = '%s Regional' % ' '.join(newwords)
            except ValueError:
                index = words.index('Refugee')
                name = '%s Regional' % ' '.join(words[:index])
    if not name:
        name = re.sub('[\(\[].*?[\)\]]', '', origname)
        name = multiple_replace(name, {'Intersectoral': '', 'Response': '', 'Plan': '', 'Joint': ''})
        name = ' '.join(name.split())
    if origname == name:
        logger.info('Plan name %s not simplified' % name)
    else:
        logger.info('Plan name %s simplified from %s' % (name, origname))
    return name


def get_fts(configuration, countryiso3s, downloader, scraper=None):
    if scraper and scraper not in inspect.currentframe().f_code.co_name:
        return list(), list(), list(), list(), list(), list()
    hrp_requirements = dict()
    hrp_funding = dict()
    hrp_percentage = dict()
    hrp_covid_requirements = dict()
    hrp_covid_funding = dict()
    hrp_covid_percentage = dict()
    other_planname = dict()
    other_requirements = dict()
    other_funding = dict()
    other_percentage = dict()

    def add_other_requirements_and_funding(iso3, name, req, fund, pct):
        dict_of_lists_add(other_planname, iso3, name)
        if req:
            dict_of_lists_add(other_requirements, iso3, req)
        else:
            dict_of_lists_add(other_requirements, iso3, None)
        if fund and req:
            dict_of_lists_add(other_funding, iso3, fund)
            dict_of_lists_add(other_percentage, iso3, pct)
        else:
            dict_of_lists_add(other_funding, iso3, None)
            dict_of_lists_add(other_percentage, iso3, None)

    fts_configuration = configuration['fts']
    v1_url = fts_configuration['v1_url']
    v2_url = fts_configuration['v2_url']

    total_covidreq = 0
    total_covidfund = 0
    rows = list()

    def add_covid_requirements_and_funding(name, includetotals, req, fund):
        nonlocal total_covidreq, total_covidfund

        if not includetotals:
            return
        if req or fund:
            rows.append([name, req, fund])
            logger.info('%s: Requirements=%d, Funding=%d' % (name, req, fund))
            if req:
                total_covidreq += req
            if fund:
                total_covidfund += fund

    url = '%sfts/flow/plan/overview/progress/%d' % (v2_url, today.year)
    data = download_data(url, downloader)
    for plan in data['plans']:
        plan_id = plan['id']
        planname = plan['name']
        allreq = plan['requirements']['revisedRequirements']
        funding = plan.get('funding')
        if funding:
            allfund = funding['totalFunding']
        else:
            allfund = None
        includetotals = plan['planType']['includeTotals']
        if plan_id == 952:
            add_covid_requirements_and_funding(planname, includetotals, allreq, allfund)
            continue
        covidreq, covidfund = get_requirements_and_funding(v1_url, v2_url, plan_id, downloader)
        add_covid_requirements_and_funding(planname, includetotals, covidreq, covidfund)

        countries = plan['countries']
        countryid_iso3mapping = dict()
        for country in countries:
            countryiso = country['iso3']
            if countryiso:
                countryid = country['id']
                countryid_iso3mapping[countryid] = countryiso
        if len(countryid_iso3mapping) == 0:
            continue
        if len(countryid_iso3mapping) == 1:
            countryiso = countryid_iso3mapping.popitem()[1]
            if not countryiso or countryiso not in countryiso3s:
                continue
            plan_type = plan['planType']['name'].lower()
            if funding:
                allpct = get_percent(funding['progress'], 100)
            else:
                allpct = None
            if plan_type == 'humanitarian response plan':
                if allreq:
                    hrp_requirements[countryiso] = allreq
                else:
                    hrp_requirements[countryiso] = None
                if allfund and allreq:
                    hrp_funding[countryiso] = allfund
                    hrp_percentage[countryiso] = allpct
                if covidreq:
                    hrp_covid_requirements[countryiso] = covidreq
                else:
                    hrp_covid_requirements[countryiso] = None
                if covidfund and covidreq:
                    hrp_covid_funding[countryiso] = covidfund
                    hrp_covid_percentage[countryiso] = get_percent(covidfund, covidreq)
            else:
                planname = map_planname(planname)
                add_other_requirements_and_funding(countryiso, planname, allreq, allfund, allpct)
        else:
            allreqs, allfunds = get_requirements_and_funding_location(v1_url, plan_id, countryid_iso3mapping, countryiso3s, downloader)
            planname = map_planname(planname)
            for countryiso in allreqs:
                allreq = allreqs[countryiso]
                allfund = allfunds.get(countryiso)
                if allfund:
                    allpct = get_percent(allfund, allreq)
                else:
                    allpct = None
                add_other_requirements_and_funding(countryiso, planname, allreq, allfund, allpct)

    def create_output(vallist):
        strings = list()
        for val in vallist:
            if val is None:
                strings.append('')
            else:
                strings.append(str(val))
        return '|'.join(strings)

    for countryiso in other_requirements:
        other_planname[countryiso] = create_output(other_planname[countryiso])
        other_requirements[countryiso] = create_output(other_requirements[countryiso])
        other_funding[countryiso] = create_output(other_funding[countryiso])
        other_percentage[countryiso] = create_output(other_percentage[countryiso])
    total_allreq = data['totals']['revisedRequirements']
    total_allfund = data['totals']['totalFunding']
    total_allpercent = get_percent(data['totals']['progress'], 100)
    total_covidpercent = get_percent(total_covidfund, total_covidreq)
    logger.info('Processed FTS')
    write_list_to_csv('ftscovid.csv', rows, ['Name', 'Requirements', 'Funding'])
    ghxltags = ['#value+funding+hrp+required+usd', '#value+funding+hrp+total+usd', '#value+funding+hrp+pct',
                '#value+covid+funding+hrp+required+usd', '#value+covid+funding+hrp+total+usd', '#value+covid+funding+hrp+pct']
    hxltags = ghxltags + ['#value+funding+other+planname', '#value+funding+other+required+usd', '#value+funding+other+total+usd', '#value+funding+other+pct']
    total_allreq = {'global': total_allreq}
    total_allfund = {'global': total_allfund}
    total_allpercent = {'global': total_allpercent}
    total_covidreq = {'global': total_covidreq}
    total_covidfund = {'global': total_covidfund}
    total_covidpercent = {'global': total_covidpercent}
    return [['RequiredFunding', 'Funding', 'PercentFunded',
             'RequiredGHRPCovidFunding', 'GHRPCovidFunding', 'GHRPCovidPercentFunded'], ghxltags], \
           [total_allreq, total_allfund, total_allpercent, total_covidreq, total_covidfund, total_covidpercent], \
           [(hxltag, today_str, 'OCHA', fts_configuration['source_url']) for hxltag in ghxltags], \
           [['RequiredHRPFunding', 'HRPFunding', 'HRPPercentFunded',
             'RequiredHRPCovidFunding', 'HRPCovidFunding', 'HRPCovidPercentFunded',
             'OtherPlans', 'RequiredOtherPlansFunding', 'OtherPlansFunding', 'OtherPlansPercentFunded'],
             hxltags], \
           [hrp_requirements, hrp_funding, hrp_percentage, hrp_covid_requirements, hrp_covid_funding, hrp_covid_percentage,
            other_planname, other_requirements, other_funding, other_percentage], \
           [(hxltag, today_str, 'OCHA', fts_configuration['source_url']) for hxltag in hxltags]
