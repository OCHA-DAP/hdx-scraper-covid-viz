import logging
from typing import Dict

from hdx.scraper.readers import read
from hdx.utilities.dateparse import default_date, parse_date
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class EducationClosures(BaseScraper):
    name = "education_closures"
    headers = {
        "national": (("School Closure",), ("#impact+type",)),
        "regional": (("No. closed countries",), ("#status+country+closed",)),
    }

    def __init__(self, today, countryiso3s, regionlookup, downloader):
        super().__init__()
        self.today = today
        self.countryiso3s = countryiso3s
        self.regionlookup = regionlookup
        self.downloader = downloader
        self.fully_closed = None

    @staticmethod
    def get_fully_closed(closures):
        fully_closed = list()
        if not closures:
            return fully_closed
        for countryiso, closure in closures.items():
            if closure.lower() == "closed due to covid-19":
                fully_closed.append(countryiso)
        return fully_closed

    def run(self, datasetinfo: Dict):
        closures_headers, closures_iterator = read(self.downloader, datasetinfo)
        closures = self.get_values("national")[0]
        closed_countries = self.get_values("regional")[0]
        country_dates = dict()
        for row in closures_iterator:
            countryiso = row["ISO"]
            if not countryiso or countryiso not in self.countryiso3s:
                continue
            date = row["Date"]
            if isinstance(date, str):
                date = parse_date(date)
            if date > self.today:
                continue
            max_date = country_dates.get(countryiso, default_date)
            if date < max_date:
                continue
            country_dates[countryiso] = date
            closures[countryiso] = row["Status"]
        fully_closed = self.get_fully_closed(closures)
        for countryiso in self.values:
            for region in self.regionlookup.iso3_to_region_and_hrp[countryiso]:
                if countryiso in fully_closed:
                    closed_countries[region] = closed_countries.get(region, 0) + 1
