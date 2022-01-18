import logging

from hdx.scraper.readers import read_hdx
from scrapers.base_scraper import BaseScraper
from scrapers.utilities import calculate_ratios

logger = logging.getLogger(__name__)


class VaccinationCampaigns(BaseScraper):
    name = "vaccination_campaigns"
    headers = {
        "national": (
            ("Vaccinations Postponed", "Vaccination Ratio"),
            ("#vaccination+postponed+num", "#vaccination+num+ratio"),
        )
    }

    def __init__(self, today, countryiso3s, downloader, outputs):
        super().__init__()
        self.today = today
        self.countryiso3s = countryiso3s
        self.downloader = downloader
        self.outputs = outputs

    def run(self, datasetinfo):
        headers, iterator = read_hdx(self.downloader, datasetinfo, today=self.today)
        hxlrow = next(iterator)
        campaigns_per_country = dict()
        affected_campaigns_per_country = self.get_values("national")[0]
        affected_campaigns_per_country2 = dict()
        for row in iterator:
            newrow = dict()
            countryiso = None
            status = None
            for key in row:
                hxltag = hxlrow[key]
                if hxltag == "":
                    continue
                value = row[key]
                newrow[hxlrow[key]] = value
                if hxltag == "#country+code":
                    countryiso = value
                elif hxltag == "#status+name":
                    status = value.lower()
            if not countryiso or countryiso not in self.countryiso3s:
                continue
            if not status or status == "completed as planned":
                continue
            self.outputs["json"].add_data_row(self.name, newrow)
            campaigns_per_country[countryiso] = (
                campaigns_per_country.get(countryiso, 0) + 1
            )
            if status != "on track" and "reinstated" not in status:
                affected_campaigns_per_country2[countryiso] = (
                    affected_campaigns_per_country2.get(countryiso, 0) + 1
                )
            if status in ("postponed covid", "cancelled"):
                affected_campaigns_per_country[countryiso] = (
                    affected_campaigns_per_country.get(countryiso, 0) + 1
                )
        for countryiso in campaigns_per_country:
            if countryiso not in affected_campaigns_per_country:
                affected_campaigns_per_country[countryiso] = 0
        ratios = self.get_values("national")[1]
        calculate_ratios(ratios, campaigns_per_country, affected_campaigns_per_country2)
        logger.info("Processed vaccination campaigns")
