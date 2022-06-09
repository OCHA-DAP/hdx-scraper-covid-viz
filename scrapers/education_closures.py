import logging
from typing import Dict

from hdx.scraper.base_scraper import BaseScraper
from hdx.utilities.dateparse import default_date, parse_date

logger = logging.getLogger(__name__)


class EducationClosures(BaseScraper):
    def __init__(self, datasetinfo: Dict, today, countryiso3s, iso3_to_region):
        super().__init__(
            "education_closures",
            datasetinfo,
            {
                "national": (("School Closure",), ("#impact+type",)),
                "regional": (
                    ("No. closed countries",),
                    ("#status+country+closed",),
                ),
            },
        )
        self.today = today
        self.countryiso3s = countryiso3s
        self.iso3_to_region = iso3_to_region
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

    def run(self) -> None:
        closures_headers, closures_iterator = self.get_reader().read(self.datasetinfo)
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
        self.fully_closed = self.get_fully_closed(closures)
        for countryiso in closures:
            for region in self.iso3_to_region[countryiso]:
                if countryiso in self.fully_closed:
                    closed_countries[region] = closed_countries.get(region, 0) + 1

    def run_after_fallbacks(self) -> None:
        self.fully_closed = self.get_fully_closed(self.get_values("national")[0])
