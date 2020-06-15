# -*- coding: utf-8 -*-
import inspect
import logging

from hdx.location.country import Country

from model.readers import read_tabular

logger = logging.getLogger(__name__)


def get_ifi(configuration, countryiso3s, downloader, scraper=None):
    if scraper and scraper not in inspect.currentframe().f_code.co_name:
        return list(), list(), list()
    url = configuration['ifi_url']
    valuedicts = [dict(), dict(), dict(), dict(), dict(), dict()]

    _, data = read_tabular(downloader, {'url': url, 'sheet': 'Master', 'headers': 2,
                                        'format': 'xlsx'})
    for row in data:
        countryname = row['Country']
        countryiso, _ = Country.get_iso3_country_code_fuzzy(countryname)
        if not countryiso:
            logger.error('Cannot get ISO3 for country %s!' % countryname)
            continue
        if countryiso not in countryiso3s:
            continue


    _, data = read_tabular(downloader, {'url': url, 'sheet': 'History log', 'headers': 1,
                                        'format': 'xlsx'})
    ignored_orgs = set()
    ignored_contribution_types = set()

    lookup = {'IMF': (0, ['IMF CCRT', 'IMF RCF', 'IMF Augmented SBA', 'IMF RFI', 'IMF Augmented ECF']),
              'WB': (1, ['WB first tranche', 'WB FTF and other financing mechanism', 'WB second tranche',
                         'WB Partial redeploying of existing projects', "WB Paid out Cat DDO's"])}

    def sum_org(countryiso, org, contribution_type, amount):
        info = lookup.get(org)
        if not info:
            ignored_orgs.add(row['IFI'])
            return
        index, contribution_types = info
        if contribution_type not in contribution_types:
            ignored_contribution_types.add(contribution_type)
            return
        curval = valuedicts[index].get(countryiso, 0.0)
        valuedicts[index][countryiso] = curval + amount

    for row in data:
        countryname = row['Country']
        countryiso, _ = Country.get_iso3_country_code_fuzzy(countryname)
        if not countryiso:
            logger.error('Cannot get ISO3 for country %s!' % countryname)
            continue
        if countryiso not in countryiso3s:
            continue
        org = row['IFI']
        contribution_type = row['Contribution type']
        amount = float(row['Amount USD (millions)'])
        sum_org(countryiso, org, contribution_type, amount)
    logger.info('The following IFIs were ignored: %s' % ', '.join(sorted(list(ignored_orgs))))
    logger.info('The following contribution types were ignored: %s' % ', '.join(sorted(list(ignored_contribution_types))))




    logger.info('Processed IFI')
    hxltags = ['#affected+refugees', '#affected+date+refugees']
    return [['TotalRefugees', 'TotalRefugeesDate'], hxltags], valuedicts, [[hxltag, today_str, 'UNHCR', configuration['unhcr_source_url']] for hxltag in hxltags]
