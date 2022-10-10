import logging

from dateutil.relativedelta import relativedelta
from hdx.scraper.base_scraper import BaseScraper
from hdx.utilities.dateparse import default_date, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


class Inform(BaseScraper):
    def __init__(self, datasetinfo, today, countryiso3s):
        super().__init__(
            "inform",
            datasetinfo,
            {
                "national": (
                    (
                        "INFORM Severity Index",
                        "INFORM Severity category",
                        "Trend over 3 months",
                    ),
                    (
                        "#severity+inform+num",
                        "#severity+inform+type",
                        "#severity+inform+trend",
                    ),
                )
            },
        )
        self.today = today
        self.countryiso3s = countryiso3s

    def download_data(self, date, base_url, input_cols, reader):
        url = base_url % date.strftime("%b%Y")
        countries_index = dict()
        while url:
            json = reader.download_json(url)
            for result in json["results"]:
                countryiso3 = result["iso3"]
                if len(countryiso3) != 1:
                    continue
                countryiso3 = countryiso3[0]
                if countryiso3 not in self.countryiso3s:
                    continue
                if result["country_level"] != "Yes":
                    continue
                first_val = result[input_cols[0]]
                if not first_val:
                    continue
                country_index = countries_index.get(countryiso3, dict())
                individual_or_aggregated = result["individual_aggregated"]
                drivers = ",".join(result["drivers"])
                ind_agg_type = country_index.get("ind_agg_type", dict())
                dict_of_lists_add(ind_agg_type, individual_or_aggregated, drivers)
                country_index["ind_agg_type"] = ind_agg_type
                crises_index = country_index.get("crises", dict())
                crisis_index = crises_index.get(drivers, dict())
                last_updated = result["Last updated"]
                for input_col in input_cols:
                    crisis_index[input_col] = (result[input_col], last_updated)
                crises_index[drivers] = crisis_index
                country_index["crises"] = crises_index
                countries_index[countryiso3] = country_index
            url = json["next"]
        return countries_index

    def get_columns_by_date(self, date, base_url, reader, crisis_drivers, not_found):
        input_col = self.get_headers("national")[0][0]
        countries_index = self.download_data(date, base_url, [input_col], reader)
        valuedict = dict()
        for countryiso3, driver in crisis_drivers.items():
            country_index = countries_index.get(countryiso3)
            if not country_index:
                not_found.add(countryiso3)
                continue
            crisis = country_index["crises"].get(driver)
            if not crisis:
                not_found.add(countryiso3)
                continue
            val, last_updated = crisis[input_col]
            valuedict[countryiso3] = val
        return valuedict

    def get_latest_columns(self, date, base_url, reader):
        input_cols = self.get_headers("national")[0][:2]
        countries_index = self.download_data(date, base_url, input_cols, reader)
        valuedicts = self.get_values("national")[:2]
        crisis_drivers = dict()
        max_date = default_date
        for countryiso3, country_data in countries_index.items():
            crises_drivers = country_data["ind_agg_type"].get("Aggregated")
            if not crises_drivers:
                crises_drivers = country_data["ind_agg_type"].get("Individual")
            drivers = crises_drivers[0]
            crisis_drivers[countryiso3] = drivers
            crisis = country_data["crises"][drivers]
            for i, input_col in enumerate(input_cols):
                val, last_updated = crisis[input_col]
                valuedicts[i][countryiso3] = val
                date = parse_date(last_updated)
                if date > max_date:
                    max_date = date
        return valuedicts, crisis_drivers, max_date

    def run(self) -> None:
        reader = self.get_reader(self.name)
        reader.read_hdx_metadata(self.datasetinfo)
        base_url = self.datasetinfo["url"]
        start_date = self.today - relativedelta(months=1)
        valuedictsfortoday, crisis_drivers, max_date = self.get_latest_columns(
            start_date, base_url, reader
        )
        severity_indices = [valuedictsfortoday[0]]
        not_found = set()
        for i in range(1, 6, 1):
            prevdate = start_date - relativedelta(months=i)
            valuedictfordate = self.get_columns_by_date(
                prevdate,
                base_url,
                reader,
                crisis_drivers,
                not_found,
            )
            severity_indices.append(valuedictfordate)

        trend_valuedict = self.get_values("national")[2]
        for countryiso3 in severity_indices[0]:
            if countryiso3 in not_found:
                trend_valuedict[countryiso3] = "-"
                continue
            avg = round(
                (
                    severity_indices[0][countryiso3]
                    + severity_indices[1][countryiso3]
                    + severity_indices[2][countryiso3]
                )
                / 3,
                1,
            )
            prevavg = round(
                (
                    severity_indices[3][countryiso3]
                    + severity_indices[4][countryiso3]
                    + severity_indices[5][countryiso3]
                )
                / 3,
                1,
            )
            if avg == prevavg:
                trend = "stable"
            elif avg < prevavg:
                trend = "decreasing"
            else:
                trend = "increasing"
            trend_valuedict[countryiso3] = trend
        self.datasetinfo["source_date"] = max_date
