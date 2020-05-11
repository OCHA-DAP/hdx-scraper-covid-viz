# -*- coding: utf-8 -*-
import logging
from difflib import get_close_matches

from unidecode import unidecode
from hdx.location.country import Country

logger = logging.getLogger(__name__)


class AdminInfo(object):
    pcodes = list()
    name_to_pcode = dict()
    pcode_to_name = dict()
    pcode_to_iso3 = dict()

    def __init__(self, configuration):
        admin_info = configuration['admin_info']
        countryiso3s = set()
        for row in admin_info:
            countryiso3 = row['alpha_3']
            countryiso3s.add(countryiso3)
            pcode = row['ADM1_PCODE']
            self.pcodes.append(pcode)
            adm1_name = row['ADM1_REF']
            self.pcode_to_name[pcode] = adm1_name
            self.name_to_pcode[unidecode(adm1_name).lower()] = pcode
            self.pcode_to_iso3[pcode] = countryiso3
        self.countryiso3s = sorted(list(countryiso3s))
        self.countryiso2s = [Country.get_iso2_from_iso3(x) for x in countryiso3s]

    def get_pcode(self, adm1_name, logtxt):
        adm1_name_lookup = unidecode(adm1_name).lower()
        pcode = self.name_to_pcode.get(adm1_name_lookup)
        if not pcode:
            for map_name in self.name_to_pcode:
                if adm1_name_lookup in map_name:
                    pcode = self.name_to_pcode[map_name]
                    logger.info(
                        '%s: Matching (substring) %s to %s on map' % (logtxt, adm1_name, self.pcode_to_name[pcode]))
                    break
        if not pcode:
            map_names = list(self.name_to_pcode.keys())
            lower_mapnames = [x.lower() for x in map_names]
            matches = get_close_matches(adm1_name_lookup, lower_mapnames, 1, 0.8)
            if not matches:
                logger.error('%s: Could not find %s in map names!' % (logtxt, adm1_name))
                return None
            index = lower_mapnames.index(matches[0])
            map_name = map_names[index]
            pcode = self.name_to_pcode[map_name]
            logger.info('%s: Matching (fuzzy) %s to %s on map' % (logtxt, adm1_name, self.pcode_to_name[pcode]))
        return pcode
