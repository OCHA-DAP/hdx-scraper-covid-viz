import logging
from typing import Dict

import hxl
from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import dict_of_lists_add
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class IOMDTM(BaseScraper):
    name = "iom_dtm"
    headers = {"subnational": (("IDPs",), ("#affected+idps+ind",))}

    def __init__(self, today, adminone, downloader):
        super().__init__()
        self.today = today
        self.adminone = adminone
        self.downloader = downloader

    def run(self, datasetinfo: Dict) -> None:
        iom_url = datasetinfo["url"]
        headers, iterator = self.downloader.get_tabular_rows(
            iom_url, headers=1, dict_form=True, format="csv"
        )
        rows = list(iterator)
        idpsdict = dict()
        for ds_row in rows:
            countryiso3 = ds_row["Country ISO"]
            dataset = Dataset.read_from_hdx(ds_row["Dataset Name"])
            if not dataset:
                logger.warning(f"No IOM DTM data for {countryiso3}.")
                continue
            url = dataset.get_resource()["url"]
            try:
                data = hxl.data(url).cache()
                data.display_tags
            except hxl.HXLException:
                logger.warning(
                    f"Could not process IOM DTM data for {countryiso3}. Maybe there are no HXL tags."
                )
                continue
            pcodes_found = False
            for row in data:
                pcode = row.get("#adm1+code")
                if pcode:
                    pcode = self.adminone.convert_pcode_length(
                        countryiso3, pcode, "iom_dtm"
                    )
                else:
                    adm2code = row.get("#adm2+code")
                    if adm2code:
                        if len(adm2code) > 4:
                            pcode = adm2code[:-2]
                        else:  # incorrectly labelled adm2 code
                            pcode = adm2code
                if not pcode:
                    adm1name = row.get("#adm1+name")
                    if adm1name:
                        pcode, _ = self.adminone.get_pcode(
                            countryiso3, adm1name, "iom_dtm"
                        )
                if not pcode:
                    location = row.get("#loc")
                    if location:
                        location = location.split(">")[-1]
                        pcode, _ = self.adminone.get_pcode(
                            countryiso3, location, "iom_dtm"
                        )
                if pcode:
                    pcode = pcode.strip().upper()
                    idps = row.get("#affected+idps+ind")
                    if idps:
                        dict_of_lists_add(idpsdict, f"{countryiso3}:{pcode}", idps)
            if not pcodes_found:
                logger.warning(f"No pcodes found for {countryiso3}.")

        idps = self.get_values("subnational")[0]
        for countrypcode in idpsdict:
            countryiso3, pcode = countrypcode.split(":")
            if pcode not in self.adminone.pcodes:
                logger.error(f"PCode {pcode} in {countryiso3} does not exist!")
            else:
                idps[pcode] = sum(idpsdict[countrypcode])
        datasetinfo["date"] = self.today
        logger.info("Processed IOM DTMs")
