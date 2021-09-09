import logging
import sys

from hdx.scraper import add_population
from hdx.scraper.readers import read_hdx
from hdx.utilities.dictandlist import dict_of_lists_add, dict_of_sets_add
from hdx.utilities.text import get_fraction_str, get_numeric_if_possible, number_format

logger = logging.getLogger(__name__)


class Region:
    def __init__(self, region_config, today, downloader, gho_countries, hrp_countries):
        self.region_config = region_config
        _, iterator = read_hdx(downloader, region_config, today=today)
        self.iso3_to_region = dict()
        self.iso3_to_region_and_hrp = dict()
        regions = set()
        for row in iterator:
            countryiso = row[region_config["iso3"]]
            if countryiso and countryiso in gho_countries:
                region = row[region_config["region"]]
                if region == "NO COVERAGE":
                    continue
                regions.add(region)
                dict_of_sets_add(self.iso3_to_region_and_hrp, countryiso, region)
                self.iso3_to_region[countryiso] = region
        self.regions = sorted(list(regions))
        region = "HRPs"
        self.regions.insert(0, region)
        for countryiso in hrp_countries:
            dict_of_sets_add(self.iso3_to_region_and_hrp, countryiso, region)
        region = "GHO"
        self.regions.insert(0, region)
        for countryiso in gho_countries:
            dict_of_sets_add(self.iso3_to_region_and_hrp, countryiso, region)
        self.hrp_countries = hrp_countries

    @staticmethod
    def get_float_or_int(valuestr):
        if not valuestr or valuestr == "N/A":
            return None
        if "." in valuestr:
            return float(valuestr)
        else:
            return int(valuestr)

    @classmethod
    def get_numeric(cls, valuestr):
        if isinstance(valuestr, str):
            total = 0
            hasvalues = False
            for value in valuestr.split("|"):
                value = cls.get_float_or_int(value)
                if value:
                    hasvalues = True
                    total += value
            if hasvalues is False:
                return ""
            return total
        return valuestr

    @staticmethod
    def get_headers_and_columns(desired_headers, input_headers, input_columns, message):
        headers = [list(), list()]
        columns = list()
        for header in desired_headers:
            try:
                index = input_headers[0].index(header)
                headers[0].append(header)
                headers[1].append(input_headers[1][index])
                columns.append(input_columns[index])
            except ValueError:
                logger.error(message.format(header))
        return headers, columns

    def should_process(self, process_info, region, countryiso):
        subset = process_info.get("subset")
        if subset:
            # "hrps" is the only subset defined right now
            if (
                subset == "hrps"
                and region != "GHO"
                and countryiso not in self.hrp_countries
            ):
                return False
        return True

    @classmethod
    def process(cls, process_info, valdicts, regional_headers, index):
        valdict = valdicts[-1]
        action = process_info["action"]
        if action == "sum" or action == "mean":
            for region, valuelist in valdict.items():
                total = ""
                novals = 0
                for valuestr in valuelist:
                    value = ""
                    if isinstance(valuestr, int) or isinstance(valuestr, float):
                        value = valuestr
                    else:
                        if valuestr:
                            value = cls.get_numeric(valuestr)
                    if value != "":
                        novals += 1
                        if total == "":
                            total = value
                        else:
                            total += value
                if action == "mean":
                    if not isinstance(total, str):
                        total /= novals
                if isinstance(total, float):
                    valdict[region] = number_format(total, trailing_zeros=False)
                else:
                    valdict[region] = total
        elif action == "range":
            for region, valuelist in valdict.items():
                min = sys.maxsize
                max = -min
                for valuestr in valuelist:
                    if valuestr:
                        value = cls.get_numeric(valuestr)
                        if value > max:
                            max = value
                        if value < min:
                            min = value
                if min == sys.maxsize or max == -sys.maxsize:
                    valdict[region] = ""
                else:
                    if isinstance(max, float):
                        max = number_format(max, trailing_zeros=False)
                    if isinstance(min, float):
                        min = number_format(min, trailing_zeros=False)
                    valdict[region] = f"{str(min)}-{str(max)}"
        elif action == "eval":
            formula = process_info["formula"]
            for region, valuelist in valdict.items():
                toeval = formula
                for j in range(index):
                    value = valdicts[j].get(region, "")
                    if value == "":
                        value = None
                    toeval = toeval.replace(regional_headers[0][j], str(value))
                valdict[region] = eval(toeval)

    def get_regional(
        self,
        regionlookup,
        national_headers,
        national_columns,
        population_lookup=None,
        *args,
    ):
        if population_lookup is None:
            process_cols = self.region_config["process_cols"]
        else:
            process_cols = {"Population": {"action": "sum"}}
        desired_headers = process_cols.keys()
        message = "Regional header {} not found in national headers!"
        regional_headers, regional_columns = self.get_headers_and_columns(
            desired_headers, national_headers, national_columns, message
        )
        valdicts = list()
        for i, header in enumerate(regional_headers[0]):
            valdict = dict()
            valdicts.append(valdict)
            process_info = process_cols[header]
            column = regional_columns[i]
            for countryiso in column:
                for region in regionlookup.iso3_to_region_and_hrp[countryiso]:
                    if not self.should_process(process_info, region, countryiso):
                        continue
                    dict_of_lists_add(valdict, region, column[countryiso])
            self.process(process_info, valdicts, regional_headers, i)

        if population_lookup is None:
            multi_cols = self.region_config.get("multi_cols", list())
            for header in multi_cols:
                multi_info = multi_cols[header]
                input_headers = multi_info["headers"]
                ignore = False
                for input_header in input_headers:
                    if input_header not in national_headers[0]:
                        logger.error(message.format(input_header))
                        ignore = True
                        break
                if ignore:
                    continue
                regional_headers[0].append(header)
                regional_headers[1].append(multi_info["hxltag"])
                found_region_countries = set()
                valdict = dict()
                valdicts.append(valdict)
                for i, orig_header in enumerate(input_headers):
                    index = national_headers[0].index(orig_header)
                    column = national_columns[index]
                    for countryiso in column:
                        for region in regionlookup.iso3_to_region_and_hrp[countryiso]:
                            if not self.should_process(multi_info, region, countryiso):
                                continue
                            key = f"{region}|{countryiso}"
                            if key in found_region_countries:
                                continue
                            value = column[countryiso]
                            if value:
                                found_region_countries.add(key)
                                dict_of_lists_add(valdict, region, value)
                self.process(
                    multi_info, valdicts, regional_headers, len(regional_headers[0]) - 1
                )

        for arg in args:
            gheaders, gvaldicts = arg
            if gheaders:
                for i, header in enumerate(gheaders[1]):
                    try:
                        j = regional_headers[1].index(header)
                    except ValueError:
                        continue
                    valdicts[j].update(gvaldicts[i])

        add_population(population_lookup, regional_headers, valdicts)
        logger.info("Processed regional")
        return regional_headers, valdicts

    def get_world(self, regional_headers, regional_columns):
        desired_headers = self.region_config["global"]
        message = "Regional header {} to be used as global not found!"
        world_headers, world_columns = self.get_headers_and_columns(
            desired_headers, regional_headers, regional_columns, message
        )
        global_columns = list()
        for column in world_columns:
            global_columns.append({"global": column["GHO"]})
        return world_headers, global_columns
