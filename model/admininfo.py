# -*- coding: utf-8 -*-
import logging
import re
import unicodedata

from hdx.hdx_configuration import Configuration
from hdx.location.country import Country
from hdx.utilities.text import multiple_replace
import pyphonetics
from unidecode import unidecode


logger = logging.getLogger(__name__)

ascii = '([^\x00-\x7F])+'


class AdminInfo(object):
    _admininfo = None
    pcodes = list()
    pcode_lengths = dict()
    name_to_pcode = dict()
    pcode_to_name = dict()
    pcode_to_iso3 = dict()

    def __init__(self):
        configuration = Configuration.read()
        admin_info = configuration['admin_info']
        self.adm1_name_replacements = configuration['adm1_name_replacements']
        self.adm1_fuzzy_ignore = configuration['adm1_fuzzy_ignore']
        iso3s_no_pcodes = set()
        countryiso3s = set()
        for row in admin_info:
            countryiso3 = row['alpha_3']
            countryiso3s.add(countryiso3)
            pcode = row.get('ADM1_PCODE')
            if not pcode:
                iso3s_no_pcodes.add(countryiso3)
                continue
            self.pcodes.append(pcode)
            self.pcode_lengths[countryiso3] = len(pcode)
            adm1_name = row['ADM1_REF']
            self.pcode_to_name[pcode] = adm1_name
            name_to_pcode = self.name_to_pcode.get(countryiso3, dict())
            name_to_pcode[unidecode(adm1_name).lower()] = pcode
            self.name_to_pcode[countryiso3] = name_to_pcode
            self.pcode_to_iso3[pcode] = countryiso3
        self.countryiso3s = sorted(list(countryiso3s))
        self.iso3s_no_pcodes = sorted(list(iso3s_no_pcodes))
        self.init_matches_errors()

    def init_matches_errors(self, scraper=None):
        self.matches = set()
        self.ignored = set()
        self.errors = set()

    def convert_pcode_length(self, countryiso3, adm1_pcode, scrapername):
        if adm1_pcode in self.pcodes:
            return adm1_pcode
        pcode_length = len(adm1_pcode)
        country_pcodelength = self.pcode_lengths.get(countryiso3)
        if not country_pcodelength:
            return None
        if pcode_length == country_pcodelength or pcode_length < 4 or pcode_length > 6:
            return None
        if country_pcodelength == 4:
            pcode = '%s%s' % (Country.get_iso2_from_iso3(adm1_pcode[:3]), adm1_pcode[-2:])
        elif country_pcodelength == 5:
            if pcode_length == 4:
                pcode = '%s0%s' % (adm1_pcode[:2], adm1_pcode[-2:])
            else:
                pcode = '%s%s' % (Country.get_iso2_from_iso3(adm1_pcode[:3]), adm1_pcode[-3:])
        elif country_pcodelength == 6:
            if pcode_length == 4:
                pcode = '%s0%s' % (Country.get_iso3_from_iso2(adm1_pcode[:2]), adm1_pcode[-2:])
            else:
                pcode = '%s%s' % (Country.get_iso3_from_iso2(adm1_pcode[:2]), adm1_pcode[-3:])
        else:
            pcode = None
        if pcode in self.pcodes:
            self.matches.add((scrapername, countryiso3, adm1_pcode, self.pcode_to_name[pcode], 'pcode length conversion'))
            return pcode
        return None

    def get_pcode(self, countryiso3, adm1_name, scrapername=None):
        if countryiso3 in self.iso3s_no_pcodes:
            self.ignored.add((scrapername, countryiso3))
            return None
        name_to_pcode = self.name_to_pcode.get(countryiso3)
        if not name_to_pcode:
            self.errors.add((scrapername, countryiso3))
            return None
        if adm1_name.lower() in self.adm1_fuzzy_ignore:
            self.ignored.add((scrapername, countryiso3, adm1_name))
            return None
        # Replace accented characters with non accented ones
        adm1_name_lookup = ''.join((c for c in unicodedata.normalize('NFD', adm1_name) if unicodedata.category(c) != 'Mn'))
        # Remove all non-ASCII characters
        adm1_name_lookup = re.sub(ascii, ' ', adm1_name_lookup)
        adm1_name_lookup = unidecode(adm1_name_lookup)
        adm1_name_lookup = adm1_name_lookup.strip().lower()
        adm1_name_lookup2 = multiple_replace(adm1_name_lookup, self.adm1_name_replacements)
        pcode = name_to_pcode.get(adm1_name_lookup, name_to_pcode.get(adm1_name_lookup2))
        if not pcode:
            for map_name in name_to_pcode:
                if adm1_name_lookup in map_name:
                    pcode = name_to_pcode[map_name]
                    self.matches.add((scrapername, countryiso3, adm1_name, self.pcode_to_name[pcode], 'substring'))
                    break
            for map_name in name_to_pcode:
                if adm1_name_lookup2 in map_name:
                    pcode = name_to_pcode[map_name]
                    self.matches.add((scrapername, countryiso3, adm1_name, self.pcode_to_name[pcode], 'substring'))
                    break
        if not pcode:
            map_names = list(name_to_pcode.keys())
            lower_mapnames = [x.lower().replace(' ', '') for x in map_names]
            rs = pyphonetics.RefinedSoundex()
            mindistance = None
            match = None
            for i, mapname in enumerate(lower_mapnames):
                distance = rs.distance(adm1_name_lookup, mapname)
                if mindistance is None or distance < mindistance:
                    mindistance = distance
                    match = i
            for i, mapname in enumerate(lower_mapnames):
                distance = rs.distance(adm1_name_lookup2, mapname)
                if mindistance is None or distance < mindistance:
                    mindistance = distance
                    match = i
            if mindistance > 2:
                self.errors.add((scrapername, countryiso3, adm1_name))
                return None
            map_name = map_names[match]
            pcode = name_to_pcode[map_name]
            self.matches.add((scrapername, countryiso3, adm1_name, self.pcode_to_name[pcode], 'fuzzy'))
        return pcode

    def output_matches(self):
        for match in sorted(self.matches):
            logger.info('%s - %s: Matching (%s) %s to %s on map' % (match[0], match[1], match[4], match[2], match[3]))

    def output_ignored(self):
        for ignored in sorted(self.ignored):
            if len(ignored) == 2:
                logger.info('%s - Ignored %s!' % (ignored[0], ignored[1]))
            else:
                logger.info('%s - %s: Ignored %s!' % (ignored[0], ignored[1], ignored[2]))

    def output_errors(self):
        for error in sorted(self.errors):
            if len(error) == 2:
                logger.error('%s - Could not find %s in map names!' % (error[0], error[1]))
            else:
                logger.error('%s - %s: Could not find %s in map names!' % (error[0], error[1], error[2]))

    @classmethod
    def get(cls):
        if not cls._admininfo:
            cls._admininfo = AdminInfo()
        return cls._admininfo
