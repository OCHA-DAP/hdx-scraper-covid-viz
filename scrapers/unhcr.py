import logging
from os.path import join

from dateutil.relativedelta import relativedelta
from hdx.utilities.dateparse import parse_date
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class UNHCR(BaseScraper):
    name = "unhcr"
    headers = {
        "national": (
            ("TotalRefugees", "TotalRefugeesDate"),
            ("#affected+refugees", "#affected+date+refugees"),
        )
    }

    def __init__(self, today, countryiso3s, downloader):
        super().__init__()
        self.today = today
        self.countryiso3s = countryiso3s
        self.downloader = downloader

    def run(self, datasetinfo):
        iso3tocode = self.downloader.download_tabular_key_value(
            join("config", "UNHCR_geocode.csv")
        )
        base_url = datasetinfo["url"]
        population_collections = datasetinfo["population_collections"]
        exclude = datasetinfo["exclude"]
        valuedicts = self.get_values("national")
        for countryiso3 in self.countryiso3s:
            if countryiso3 in exclude:
                continue
            code = iso3tocode.get(countryiso3)
            if not code:
                continue
            for population_collection in population_collections:
                r = self.downloader.download(base_url % (population_collection, code))
                data = r.json()["data"][0]
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
        datasetinfo["date"] = self.today
        logger.info("Processed UNHCR")
