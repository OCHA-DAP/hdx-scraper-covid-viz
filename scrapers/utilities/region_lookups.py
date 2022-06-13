import logging

from hdx.scraper.utilities.reader import Read
from hdx.utilities.dictandlist import dict_of_sets_add

logger = logging.getLogger(__name__)


class RegionLookups:
    gho_iso3_to_region = dict()
    hrp_iso3_to_region = dict()
    regions = None

    @classmethod
    def load(cls, region_config, gho_countries, hrp_countries):
        _, iterator = Read.get_reader().read_hdx(
            region_config,
            file_prefix="regions",
        )
        regions = set()
        for row in iterator:
            countryiso = row[region_config["iso3"]]
            if countryiso and countryiso in gho_countries:
                region = row[region_config["region"]]
                if region == "NO COVERAGE":
                    continue
                regions.add(region)
                dict_of_sets_add(cls.gho_iso3_to_region, countryiso, region)
                if countryiso in hrp_countries:
                    dict_of_sets_add(cls.hrp_iso3_to_region, countryiso, region)
        cls.regions = sorted(list(regions))
        region = "HRPs"
        cls.regions.insert(0, region)
        for countryiso in hrp_countries:
            dict_of_sets_add(cls.gho_iso3_to_region, countryiso, region)
            dict_of_sets_add(cls.hrp_iso3_to_region, countryiso, region)
        region = "GHO"
        cls.regions.insert(0, region)
        for countryiso in gho_countries:
            dict_of_sets_add(cls.gho_iso3_to_region, countryiso, region)
            dict_of_sets_add(cls.hrp_iso3_to_region, countryiso, region)
