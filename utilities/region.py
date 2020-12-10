from hdx.utilities.dictandlist import dict_of_sets_add

from utilities.readers import read_hdx


class Region(object):
    def __init__(self, downloader, region_config, h63, h25):
        _, iterator = read_hdx(downloader, region_config)
        self.iso3_to_region = dict()
        self.iso3_to_region_and_hrp = dict()
        regions = set()
        for row in iterator:
            countryiso = row[region_config['iso3']]
            if countryiso and countryiso in h63:
                region = row[region_config['region']]
                if region == 'NO COVERAGE':
                    continue
                regions.add(region)
                dict_of_sets_add(self.iso3_to_region_and_hrp, countryiso, region)
                self.iso3_to_region[countryiso] = region
        self.regions = sorted(list(regions))
        region = 'H25'
        self.regions.insert(0, region)
        for countryiso in h25:
            dict_of_sets_add(self.iso3_to_region_and_hrp, countryiso, region)
        region = 'H63'
        self.regions.insert(0, region)
        for countryiso in h63:
            dict_of_sets_add(self.iso3_to_region_and_hrp, countryiso, region)

