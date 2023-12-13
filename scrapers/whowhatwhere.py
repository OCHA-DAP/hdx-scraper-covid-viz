import logging

from hdx.data.hdxobject import HDXError
from hdx.scraper.base_scraper import BaseScraper
from hdx.utilities.dictandlist import dict_of_sets_add

logger = logging.getLogger(__name__)


class WhoWhatWhere(BaseScraper):
    def __init__(self, datasetinfo, today, adminone):
        super().__init__(
            "whowhatwhere",
            datasetinfo,
            {"subnational": (("OrgCountAdm1",), ("#org+count+num",))},
        )
        self.today = today
        self.adminone = adminone

    def run(self) -> None:
        threew_url = self.datasetinfo["url"]
        reader = self.get_reader()
        headers, iterator = reader.get_tabular_rows(
            threew_url, headers=1, dict_form=True, format="csv", file_prefix=self.name
        )
        rows = list(iterator)
        orgdict = dict()
        for ds_row in rows:
            countryiso3 = ds_row["Country ISO"]
            dataset_name = ds_row["Dataset Name"]
            if not dataset_name:
                logger.warning(f"No 3w data for {countryiso3}.")
                continue
            try:
                dataset = reader.read_dataset(dataset_name)
                resource = dataset.get_resource()
            except HDXError:
                logger.warning(
                    f"Could not download resource data for {countryiso3}. Check dataset name."
                )
                continue
            data = reader.read_hxl_resource(resource, file_prefix=self.name)
            if data is None:
                continue
            self.source_urls.add(dataset.get_hdx_url())
            pcodes_found = False
            for row in data:
                pcode = row.get("#adm1+code")
                if not pcode:
                    adm2code = row.get("#adm2+code")
                    if adm2code:
                        if len(adm2code) > 4:
                            pcode = adm2code[:-2]
                        else:  # incorrectly labelled adm2 code
                            pcode = adm2code
                if not pcode:
                    adm1name = row.get("#adm1+name")
                    if adm1name and adm1name != 42:  # 42 is N/A in Excel
                        pcode, _ = self.adminone.get_pcode(countryiso3, adm1name, "3W")
                if not pcode:
                    location = row.get("#loc")
                    if location and location != 42:  # 42 is N/A in Excel
                        location = location.split(">")[-1]
                        pcode, _ = self.adminone.get_pcode(countryiso3, location, "3W")
                if pcode:
                    pcode = pcode.strip().upper()
                    if pcode not in self.adminone.get_pcode_list() and len(
                        pcode
                    ) != self.adminone.get_pcode_length(countryiso3):
                        pcode = self.adminone.convert_admin1_pcode_length(
                            countryiso3, pcode, "whowhatwhere"
                        )
                    org = row.get("#org")
                    if org:
                        org = org.strip().lower()
                        if org not in ["unknown", "n/a", "-"]:
                            dict_of_sets_add(orgdict, f"{countryiso3}:{pcode}", org)
                            pcodes_found = True
            if not pcodes_found:
                logger.warning(f"No pcodes found for {countryiso3}.")

        orgcount = self.get_values("subnational")[0]
        for countrypcode in orgdict:
            countryiso3, pcode = countrypcode.split(":")
            if pcode not in self.adminone.get_pcode_list():
                logger.error(f"PCode {pcode} in {countryiso3} does not exist!")
            else:
                orgcount[pcode] = len(orgdict[countrypcode])
        self.datasetinfo["source_date"] = self.today
        self.datasetinfo["source_url"] = threew_url

    def add_source_urls(self) -> None:
        pass
