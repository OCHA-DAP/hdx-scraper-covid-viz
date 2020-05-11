import logging
from difflib import get_close_matches

from hdx.utilities.dictandlist import dict_of_lists_add
from unidecode import unidecode

logger = logging.getLogger(__name__)


def get_ipc(configuration, admininfo, downloader):
    url = configuration['ipc_url']
    phases = [dict(), dict(), dict(), dict(), dict(), dict()]
    adm1phases = [dict(), dict(), dict(), dict(), dict(), dict()]
    for countryiso2 in admininfo.countryiso2s:
        response = downloader.download(url % countryiso2)
        json = response.json()
        samedate = None
        samelevel = None
        for row in json:
            date = row['date']
            if samedate is None:
                samedate = date
            elif samedate != date:
                raise ValueError('Multiple dates returned!')
            adm1_name = row['group_name']
            if adm1_name:
                at_adm1 = False
            else:
                at_adm1 = True
                adm1_name = row['area']
            if samelevel is None:
                samelevel = at_adm1
            elif samelevel != at_adm1:
                raise ValueError('Group name not consistently filled!')
            pcode = admininfo.get_pcode(adm1_name, countryiso2)
            if not pcode:
                continue
            for phase, phasedict in enumerate(adm1phases):
                index = phase + 1
                percentage = row['phase%d_C_percentage' % index]
                population = row['phase%d_C_population' % index]
                dict_of_lists_add(phasedict, pcode, (percentage, population))
    for phase, phasedict in enumerate(adm1phases):
        for pcode in phasedict:
            numerator = 0
            denominator = 0
            for percentage, population in phasedict[pcode]:
                numerator += 100 * percentage * population
                denominator += population
            if denominator == 0:
                logger.error('No population for %s!' % pcode)
            else:
                phases[phase][pcode] = numerator // denominator

    return [['FoodInsecurityP1', 'FoodInsecurityP2', 'FoodInsecurityP3', 'FoodInsecurityP4', 'FoodInsecurityP5', 'FoodInsecurityP6'],
            ['#affected+food+p1+pct', '#affected+food+p2+pct', '#affected+food+p3+pct', '#affected+food+p4+pct', '#affected+food+p5+pct', '#affected+food+p6+pct']], phases
