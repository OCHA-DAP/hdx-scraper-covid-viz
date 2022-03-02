import logging
import sys

from hdx.scraper.base_scraper import BaseScraper
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import get_fraction_str, get_numeric_if_possible, number_format

logger = logging.getLogger(__name__)


class RegionAggregation(BaseScraper):
    national_values = dict()
    regional_scrapers = list()

    @classmethod
    def get_regional_scrapers(
        cls, region_config, hrp_countries, iso3_to_region_and_hrp, runner
    ):
        cls.hrp_countries = hrp_countries
        cls.iso3_to_region_and_hrp = iso3_to_region_and_hrp
        cls.runner = runner
        process_cols = region_config["process_cols"]
        national_results = runner.get_results(levels="national", has_run=False)["national"]
        national_headers = national_results["headers"]
        national_values = national_results["values"]
        for index, national_header in enumerate(national_headers[0]):
            cls.national_values[national_header] = national_values[index]
        for header, process_info in process_cols.items():
            name = f"{header.lower()}_regional"
            input_headers = process_info.get("headers")
            if input_headers:
                exists = True
                for i, input_header in enumerate(input_headers):
                    try:
                        national_headers[0].index(input_header)
                    except ValueError:
                        logger.error(
                            f"Regional header {header} not found in national headers!"
                        )
                        exists = False
                        break
                if not exists:
                    continue
                headers = ((header,), (process_info["hxltag"],))
                scraper = RegionAggregation(name, process_info, {"regional": headers})
                cls.regional_scrapers.append(scraper)
            else:
                try:
                    index = national_headers[0].index(header)
                    headers = ((header,), (national_headers[1][index],))
                    scraper = RegionAggregation(
                        name, process_info, {"regional": headers}
                    )
                    cls.regional_scrapers.append(scraper)
                except ValueError:
                    logger.error(
                        f"Regional header {header} not found in national headers!"
                    )
        return cls.regional_scrapers

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

    def process(self, output_values):
        action = self.datasetinfo["action"]
        if action == "sum" or action == "mean":
            for region, valuelist in output_values.items():
                total = ""
                novals = 0
                for valuestr in valuelist:
                    value = ""
                    if isinstance(valuestr, int) or isinstance(valuestr, float):
                        value = valuestr
                    else:
                        if valuestr:
                            value = self.get_numeric(valuestr)
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
                    output_values[region] = number_format(total, trailing_zeros=False)
                else:
                    output_values[region] = total
        elif action == "range":
            for region, valuelist in output_values.items():
                min = sys.maxsize
                max = -min
                for valuestr in valuelist:
                    if valuestr:
                        value = self.get_numeric(valuestr)
                        if value > max:
                            max = value
                        if value < min:
                            min = value
                if min == sys.maxsize or max == -sys.maxsize:
                    output_values[region] = ""
                else:
                    if isinstance(max, float):
                        max = number_format(max, trailing_zeros=False)
                    if isinstance(min, float):
                        min = number_format(min, trailing_zeros=False)
                    output_values[region] = f"{str(min)}-{str(max)}"
        elif action == "eval":
            formula = self.datasetinfo["formula"]
            for region, valuelist in output_values.items():
                toeval = formula
                for regional_scraper in self.regional_scrapers:
                    header = regional_scraper.get_headers("regional")[0]
                    values = regional_scraper.get_values("regional")[0]
                    value = values.get(region, "")
                    if value == "":
                        value = None
                    toeval = toeval.replace(header, str(value))
                output_values[region] = eval(toeval)

    def run(self):
        regional_header = self.get_headers("regional")
        output_header = regional_header[0][0]
        output_valdicts = self.get_values("regional")
        output_values = output_valdicts[0]
        input_valdicts = list()
        input_headers = self.datasetinfo.get("headers", [output_header])
        for input_header in input_headers:
            input_valdicts.append(self.national_values[input_header])
        found_region_countries = set()
        for input_values in input_valdicts:
            for countryiso in input_values:
                for region in self.iso3_to_region_and_hrp[countryiso]:
                    if not self.should_process(self.datasetinfo, region, countryiso):
                        continue
                    key = f"{region}|{countryiso}"
                    if key in found_region_countries:
                        continue
                    value = input_values[countryiso]
                    if value:
                        found_region_countries.add(key)
                        dict_of_lists_add(output_values, region, value)
        self.process(output_values)
        # for arg in args:
        #     gheaders, gvaldicts = arg
        #     if gheaders:
        #         for i, header in enumerate(gheaders[1]):
        #             try:
        #                 j = regional_headers[1].index(header)
        #             except ValueError:
        #                 continue
        #             valdicts[j].update(gvaldicts[i])
        #

    def get_world(self, regional_headers, regional_columns):
        desired_headers = self.datasetinfo["global"]
        message = "Regional header {} to be used as global not found!"
        world_headers, world_columns = self.get_headers_and_columns(
            desired_headers, regional_headers, regional_columns, message
        )
        global_columns = list()
        for column in world_columns:
            global_columns.append({"global": column.get("GHO")})
        return world_headers, global_columns

    def add_sources(self):
        pass
