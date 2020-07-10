# -*- coding: utf-8 -*-
import logging
import re
import unicodedata

from hdx.hdx_configuration import Configuration
from hdx.location.country import Country
from hdx.utilities.text import multiple_replace
import pyphonetics
from unidecode import unidecode

from model.readers import read_hdx

logger = logging.getLogger(__name__)

ascii = '([^\x00-\x7F])+'
match_threshold = 2


class AdminInfo(object):
    _admininfo = None
    pcodes = list()
    pcode_lengths = dict()
    name_to_pcode = dict()
    pcode_to_name = dict()
    pcode_to_iso3 = dict()

    def __init__(self, downloader):
        configuration = Configuration.read()
        admin_info = configuration['admin_info']
        self.adm1_name_replacements = configuration['adm1_name_replacements']
        self.adm1_fuzzy_ignore = configuration['adm1_fuzzy_ignore']
        self.adm_mappings = configuration.get('adm_mappings', [dict(), dict()])
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
        self.adms = [self.countryiso3s, self.pcodes]
        self.iso3s_no_pcodes = sorted(list(iso3s_no_pcodes))
        self.regions, self.iso3_to_region = self.read_regional(configuration, self.countryiso3s, downloader)
        self.init_matches_errors()

    @staticmethod
    def read_regional(configuration, countryiso3s, downloader):
        regional_config = configuration['regional']
        _, iterator = read_hdx(downloader, regional_config)
        iso3_to_region = dict()
        regions = set()
        for row in iterator:
            countryiso = row[regional_config['iso3']]
            if countryiso and countryiso in countryiso3s:
                region = row[regional_config['region']]
                if region == 'NO COVERAGE':
                    continue
                regions.add(region)
                iso3_to_region[countryiso] = region
        return regions, iso3_to_region

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

    def fuzzy_pcode(self, countryiso3, adm1_name, scrapername=None):
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
            lower_mapnames = [x.lower() for x in map_names]
            rs = pyphonetics.RefinedSoundex()
            mindistance = None
            match = None

            def check_name(lookup, mapname, index):
                nonlocal mindistance, match

                distance = rs.distance(lookup, mapname)
                if mindistance is None or distance < mindistance:
                    mindistance = distance
                    match = index

            for i, mapname in enumerate(lower_mapnames):
                check_name(adm1_name_lookup, mapname, i)
            for i, mapname in enumerate(lower_mapnames):
                if mapname[:3] == 'al ':
                    check_name(adm1_name_lookup, 'ad %s' % mapname[3:], i)
                    check_name(adm1_name_lookup, mapname[3:], i)
                check_name(adm1_name_lookup2, mapname, i)

            if mindistance is None or mindistance > match_threshold:
                self.errors.add((scrapername, countryiso3, adm1_name))
                return None

            map_name = map_names[match]
            pcode = name_to_pcode[map_name]
            self.matches.add((scrapername, countryiso3, adm1_name, self.pcode_to_name[pcode], 'fuzzy'))
        return pcode

    def get_pcode(self, countryiso3, adm1_name, scrapername=None):
        pcode = self.adm_mappings[1].get(adm1_name)
        if pcode and self.pcode_to_iso3[pcode] == countryiso3:
            return pcode, True
        pcode = self.convert_pcode_length(countryiso3, adm1_name, scrapername)
        if pcode:
            adm = pcode
            exact = True
        else:
            adm = self.fuzzy_pcode(countryiso3, adm1_name, scrapername)
            exact = False
        return adm, exact

    def get_adm(self, adms, i, scrapername):
        adm = adms[i]
        if adm in self.adms[i]:
            exact = True
        else:
            exact = False
            if i == 0:
                mappingadm = self.adm_mappings[0].get(adm)
                if mappingadm:
                    adms[i] = mappingadm
                    return True
                adms[i], _ = Country.get_iso3_country_code_fuzzy(adm)
                exact = False
            elif i == 1:
                adms[i], exact = self.get_pcode(adms[0], adm, scrapername)
            else:
                adms[i] = None
            if adms[i] not in self.adms[i]:
                adms[i] = None
        return exact

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
    def setup(cls, downloader):
        if not cls._admininfo:
            cls._admininfo = AdminInfo(downloader)
        return cls._admininfo

    @classmethod
    def get(cls):
        if not cls._admininfo:
            raise ValueError('AdminInfo not set up yet')
        return cls._admininfo
