# -*- coding: utf-8 -*-
import inspect
import logging
import re

from dateutil.relativedelta import relativedelta
from hdx.utilities.dictandlist import write_list_to_csv, dict_of_lists_add
from hdx.utilities.downloader import Download
from hdx.utilities.text import multiple_replace, get_fraction_str

logger = logging.getLogger(__name__)


class FTSException(Exception):
    pass


gbv_id = 13


def download(url, downloader):
    r = downloader.download(url)
    json = r.json()
    status = json['status']
    if status != 'ok':
        raise FTSException('%s gives status %s' % (url, status))
    return json


def download_data(url, downloader):
    return download(url, downloader)['data']


def get_gbv_funding(base_url, plan_id, downloader):
    gbvfund = 0
    url = f'{base_url}1/fts/flow/custom-search?planid={plan_id}&groupby=globalcluster'
    data = download_data(url, downloader)
    fundingobjects = data['report3']['fundingTotals']['objects']
    if len(fundingobjects) != 0:
        singlefundingobjects = fundingobjects[0].get('singleFundingObjects')
        if singlefundingobjects:
            for fundobj in singlefundingobjects:
                fund_id = fundobj.get('id')
                fund = fundobj['totalFunding']
                if fund_id and fund_id == gbv_id:
                    gbvfund += fund
        sharedfundingobjects = fundingobjects[0].get('sharedFundingObjects')
        if sharedfundingobjects:
            for fundobj in sharedfundingobjects:
                fund_ids = fundobj.get('id')
                fund = fundobj['totalFunding']
                if fund_ids:
                    match = True
                    for fund_id in fund_ids:
                        if int(fund_id) != gbv_id:
                            match = False
                            break
                    if match:
                        gbvfund += fund
    return gbvfund


def get_covid_funding(plan_id, fundingobjects):
    covidfund = 0
    if len(fundingobjects) != 0:
        objectsbreakdown = fundingobjects[0].get('objectsBreakdown')
        if objectsbreakdown:
            for fundobj in objectsbreakdown:
                fund_id = fundobj.get('id')
                fund = fundobj['totalFunding']
                if fund_id and fund_id == plan_id:
                    covidfund += fund
    return covidfund


def get_requirements_and_funding_location(base_url, plan, countryid_iso3mapping, countryiso3s, downloader):
    allreqs, allfunds = dict(), dict()
    plan_id = plan['id']
    url = f'{base_url}1/fts/flow/custom-search?planid={plan_id}&groupby=location'
    data = download_data(url, downloader)
    requirements = data['requirements']
    totalreq = requirements['totalRevisedReqs']
    countryreq_is_totalreq = True
    for reqobj in requirements['objects']:
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
            if req != totalreq:
                countryreq_is_totalreq = False
    if countryreq_is_totalreq:
        allreqs = dict()
        logger.info(f'{plan_id} has same country requirements as total requirements!')

    fundingobjects = data['report3']['fundingTotals']['objects']
    if len(fundingobjects) != 0:
        objectsbreakdown = fundingobjects[0].get('objectsBreakdown')
        if objectsbreakdown:
            for fundobj in objectsbreakdown:
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


def get_fts(basic_auths, configuration, today, today_str, countryiso3s, scrapers=None):
    name = inspect.currentframe().f_code.co_name
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list(), list(), list(), list()
    hrp_requirements = dict()
    hrp_funding = dict()
    hrp_percentage = dict()
    hrp_covid_funding = dict()
    hrp_gbv_funding = dict()
    other_planname = dict()
    other_requirements = dict()
    other_funding = dict()
    other_percentage = dict()

    def add_other_requirements_and_funding(iso3, name, req, fund, pct):
        dict_of_lists_add(other_planname, iso3, name)
        if req:
            dict_of_lists_add(other_requirements, iso3, req)
            if fund:
                dict_of_lists_add(other_percentage, iso3, pct)
            else:
                dict_of_lists_add(other_percentage, iso3, None)
        else:
            dict_of_lists_add(other_requirements, iso3, None)
            dict_of_lists_add(other_percentage, iso3, None)
        if fund:
            dict_of_lists_add(other_funding, iso3, fund)
        else:
            dict_of_lists_add(other_funding, iso3, None)

    fts_configuration = configuration['fts']
    base_url = fts_configuration['url']

    total_gbvfund = 0
    rows = list()

    def add_covid_gbv_requirements_and_funding(name, includetotals, fund, gbvfund=None):
        nonlocal total_gbvfund

        if not includetotals:
            return
        if fund or gbvfund:
            rows.append([name, fund, gbvfund])
            if fund:
                logger.info('%s: Funding=%d' % (name, fund))
            if gbvfund:
                logger.info('%s: GBV Funding=%d' % (name, gbvfund))
                total_gbvfund += gbvfund

    with Download(basic_auth=basic_auths.get('fts'), rate_limit={'calls': 1, 'period': 1}) as downloader:
        curdate = today - relativedelta(months=1)
        url = f'{base_url}2/fts/flow/plan/overview/progress/{curdate.year}'
        data = download_data(url, downloader)
        plans = data['plans']
        plan_ids = ','.join([str(plan['id']) for plan in plans])
        url = f'{base_url}1/fts/flow/custom-search?emergencyid=911&planid={plan_ids}&groupby=plan'
        funding_data = download_data(url, downloader)
        fundingtotals = funding_data['report3']['fundingTotals']
        fundingobjects = fundingtotals['objects']
        total_covidfund = fundingtotals['total']
        for plan in plans:
            plan_id = str(plan['id'])
            planname = plan['name']
            allreq = plan['requirements']['revisedRequirements']
            funding = plan.get('funding')
            if funding:
                allfund = funding['totalFunding']
            else:
                allfund = None
            includetotals = plan['planType']['includeTotals']
            gbvfund = get_gbv_funding(base_url, plan_id, downloader)
            if plan.get('customLocationCode') == 'COVD':
                add_covid_gbv_requirements_and_funding(planname, includetotals, allfund, gbvfund)
                continue
            covidfund = get_covid_funding(plan_id, fundingobjects)
            add_covid_gbv_requirements_and_funding(planname, includetotals, covidfund, gbvfund)

            countries = plan['countries']
            countryid_iso3mapping = dict()
            for country in countries:
                countryiso = country['iso3']
                if countryiso:
                    countryid = country['id']
                    countryid_iso3mapping[str(countryid)] = countryiso
            if len(countryid_iso3mapping) == 0:
                continue
            if len(countryid_iso3mapping) == 1:
                countryiso = countryid_iso3mapping.popitem()[1]
                if not countryiso or countryiso not in countryiso3s:
                    continue
                plan_type = plan['planType']['name'].lower()
                if funding:
                    allpct = get_fraction_str(funding['progress'], 100)
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
                    if covidfund:
                        hrp_covid_funding[countryiso] = covidfund
                    if gbvfund:
                        hrp_gbv_funding[countryiso] = gbvfund
                else:
                    planname = map_planname(planname)
                    add_other_requirements_and_funding(countryiso, planname, allreq, allfund, allpct)
            else:
                allreqs, allfunds = get_requirements_and_funding_location(base_url, plan, countryid_iso3mapping, countryiso3s, downloader)
                planname = map_planname(planname)
                for countryiso in allfunds:
                    allfund = allfunds[countryiso]
                    allreq = allreqs.get(countryiso)
                    if allreq:
                        allpct = get_fraction_str(allfund, allreq)
                    else:
                        allpct = None
                    add_other_requirements_and_funding(countryiso, planname, allreq, allfund, allpct)
                for countryiso in allreqs:
                    if countryiso in allfunds:
                        continue
                    add_other_requirements_and_funding(countryiso, planname, allreqs[countryiso], None, None)

        def create_output(vallist):
            strings = list()
            for val in vallist:
                if val is None:
                    strings.append('')
                else:
                    strings.append(str(val))
            return '|'.join(strings)

        for countryiso in other_planname:
            other_planname[countryiso] = create_output(other_planname[countryiso])
            other_requirements[countryiso] = create_output(other_requirements[countryiso])
            other_funding[countryiso] = create_output(other_funding[countryiso])
            other_percentage[countryiso] = create_output(other_percentage[countryiso])
        total_allreq = data['totals']['revisedRequirements']
        total_allfund = data['totals']['totalFunding']
        total_allpercent = get_fraction_str(data['totals']['progress'], 100)
        logger.info('Processed FTS')
        write_list_to_csv('ftscovid.csv', rows, ['Name', 'Requirements', 'Funding'])
        ghxltags = ['#value+funding+hrp+required+usd', '#value+funding+hrp+total+usd', '#value+funding+hrp+pct',
                    '#value+covid+funding+hrp+total+usd', '#value+funding+gbv+hrp+total+usd']
        hxltags = ghxltags + ['#value+funding+other+planname', '#value+funding+other+required+usd', '#value+funding+other+total+usd', '#value+funding+other+pct']
        total_allreq = {'global': total_allreq}
        total_allfund = {'global': total_allfund}
        total_allpercent = {'global': total_allpercent}
        total_covidfund = {'global': total_covidfund}
        total_gbvfund = {'global': total_gbvfund}
        return [['RequiredFunding', 'Funding', 'PercentFunded', 'HRPCovidFunding', 'GHRPGBVFunding'], ghxltags], \
               [total_allreq, total_allfund, total_allpercent, total_covidfund, total_gbvfund], \
               [(hxltag, today_str, 'OCHA', fts_configuration['source_url']) for hxltag in ghxltags], \
               [['RequiredHRPFunding', 'HRPFunding', 'HRPPercentFunded', 'HRPCovidFunding', 'HRPGBVFunding',
                 'OtherPlans', 'RequiredOtherPlansFunding', 'OtherPlansFunding', 'OtherPlansPercentFunded'],
                 hxltags], \
               [hrp_requirements, hrp_funding, hrp_percentage, hrp_covid_funding, hrp_gbv_funding,
                other_planname, other_requirements, other_funding, other_percentage], \
               [(hxltag, today_str, 'OCHA', fts_configuration['source_url']) for hxltag in hxltags]
