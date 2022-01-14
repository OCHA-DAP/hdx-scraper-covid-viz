import logging

from hdx.scraper.readers import read
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import get_numeric_if_possible
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class CovaxDeliveries(BaseScraper):
    name = "covax_deliveries"
    headers = {
        "national": (
            ("Vaccine", "Funder", "Doses"),
            (
                "#meta+vaccine+producer",
                "#meta+vaccine+funder",
                "#capacity+vaccine+doses",
            ),
        )
    }

    def __init__(self, today, countryiso3s, downloader):
        super().__init__()
        self.today = today
        self.countryiso3s = countryiso3s
        self.downloader = downloader

    def run(self, datasetinfo):
        headers, iterator = read(self.downloader, datasetinfo, today=self.today)
        hxlrow = next(iterator)
        doses_lookup = dict()
        for row in iterator:
            newrow = dict()
            for key in row:
                newrow[hxlrow[key]] = row[key]
            countryiso = newrow["#country+code"]
            if not countryiso or countryiso not in self.countryiso3s:
                continue
            key = f'{countryiso}|{newrow["#meta+vaccine+pipeline"]}|{newrow["#meta+vaccine+producer"]}|{newrow["#meta+vaccine+funder"]}'
            nodoses = get_numeric_if_possible(newrow["#capacity+vaccine+doses"])
            if nodoses:
                doses_lookup[key] = doses_lookup.get(key, 0) + nodoses
        funders = self.get_values("national")[0]
        producers = self.get_values("national")[1]
        doses = self.get_values("national")[2]
        for key in sorted(doses_lookup):
            countryiso, pipeline, producer, funder = key.split("|")
            if pipeline == "COVAX":
                funder = f"{pipeline}/{funder}"
            dict_of_lists_add(producers, countryiso, producer)
            dict_of_lists_add(funders, countryiso, funder)
            dict_of_lists_add(doses, countryiso, str(doses_lookup[key]))
        for countryiso in funders:
            producers[countryiso] = "|".join(producers[countryiso])
            funders[countryiso] = "|".join(funders[countryiso])
            doses[countryiso] = "|".join(doses[countryiso])
        logger.info("Processed covax deliveries")
