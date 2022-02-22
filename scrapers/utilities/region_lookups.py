import logging

from hdx.scraper.utilities.readers import read_hdx
from hdx.utilities.dictandlist import dict_of_sets_add

logger = logging.getLogger(__name__)


class RegionLookups:
    iso3_to_region = dict()
    iso3_to_region_and_hrp = dict()
    regions = None

    @classmethod
    def load(cls, region_config, today, downloader, gho_countries, hrp_countries):
        _, iterator = read_hdx(downloader, region_config, today=today)
        regions = set()
        for row in iterator:
            countryiso = row[region_config["iso3"]]
            if countryiso and countryiso in gho_countries:
                region = row[region_config["region"]]
                if region == "NO COVERAGE":
                    continue
                regions.add(region)
                dict_of_sets_add(cls.iso3_to_region_and_hrp, countryiso, region)
                cls.iso3_to_region[countryiso] = region
        cls.regions = sorted(list(regions))
        region = "HRPs"
        cls.regions.insert(0, region)
        for countryiso in hrp_countries:
            dict_of_sets_add(cls.iso3_to_region_and_hrp, countryiso, region)
        region = "GHO"
        cls.regions.insert(0, region)
        for countryiso in gho_countries:
            dict_of_sets_add(cls.iso3_to_region_and_hrp, countryiso, region)
