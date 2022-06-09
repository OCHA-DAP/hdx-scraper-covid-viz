import logging
from os.path import join

from dateutil.relativedelta import relativedelta
from hdx.scraper.base_scraper import BaseScraper
from hdx.utilities.dateparse import parse_date

logger = logging.getLogger(__name__)


class UNHCR(BaseScraper):
    def __init__(self, datasetinfo, today, countryiso3s):
        super().__init__(
            "unhcr",
            datasetinfo,
            {
                "national": (
                    ("TotalRefugees", "TotalRefugeesDate"),
                    ("#affected+refugees", "#affected+date+refugees"),
                )
            },
        )
        self.today = today
        self.countryiso3s = countryiso3s

    def run(self):
        reader = self.get_reader()
        iso3tocode = reader.downloader.download_tabular_key_value(
            join("config", "UNHCR_geocode.csv")
        )
        base_url = self.datasetinfo["url"]
        population_collections = self.datasetinfo["population_collections"]
        exclude = self.datasetinfo["exclude"]
        valuedicts = self.get_values("national")
        for countryiso3 in self.countryiso3s:
            if countryiso3 in exclude:
                continue
            code = iso3tocode.get(countryiso3)
            if not code:
                continue
            for population_collection in population_collections:
                url = base_url % (population_collection, code)
                logger.info(f"Downloading {url}")
                json = reader.download_json(url)
                data = json["data"][0]
                individuals = data["individuals"]
                if individuals is None:
                    continue
                date = data["date"]
                if parse_date(date) < self.today - relativedelta(years=2):
                    continue
                existing_individuals = valuedicts[0].get(countryiso3)
                if existing_individuals is None:
                    valuedicts[0][countryiso3] = int(individuals)
                    valuedicts[1][countryiso3] = date
                else:
                    valuedicts[0][countryiso3] += int(individuals)
        self.datasetinfo["source_date"] = self.today
