import inspect
import logging

import hxl
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.dictandlist import dict_of_sets_add

logger = logging.getLogger(__name__)


def get_whowhatwhere(configuration, today_str, adminone, downloader, scrapers=None):
    name = inspect.currentframe().f_code.co_name
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list()
    threew_url = configuration["3w_url"]
    headers, iterator = downloader.get_tabular_rows(
        threew_url, headers=1, dict_form=True, format="csv"
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
            dataset = Dataset.read_from_hdx(dataset_name)
            resource = dataset.get_resource(0)
        except HDXError:
            logger.warning(
                f"Could not download resource data for {countryiso3}. Check dataset name."
            )
        try:
            data = hxl.data(resource["url"]).cache()
            data.display_tags
        except hxl.HXLException:
            logger.warning(
                f"Could not process 3w data for {countryiso3}. Maybe there are no HXL tags."
            )
            continue
        except Exception:
            logger.exception(f"Error reading 3w data for {countryiso3}!")
            raise
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
                if adm1name:
                    pcode, _ = adminone.get_pcode(countryiso3, adm1name, "3W")
            if not pcode:
                location = row.get("#loc")
                if location:
                    location = location.split(">")[-1]
                    pcode, _ = adminone.get_pcode(countryiso3, location, "3W")
            if pcode:
                pcode = pcode.strip().upper()
                if pcode not in adminone.pcodes and len(pcode) != adminone.pcode_lengths.get(countryiso3):
                    pcode = adminone.convert_pcode_length(countryiso3, pcode, "whowhatwhere")
                org = row.get("#org")
                if org:
                    org = org.strip().lower()
                    if org not in ["unknown", "n/a", "-"]:
                        dict_of_sets_add(orgdict, f"{countryiso3}:{pcode}", org)
                        pcodes_found = True
        if not pcodes_found:
            logger.warning(f"No pcodes found for {countryiso3}.")

    orgcount = dict()
    for countrypcode in orgdict:
        countryiso3, pcode = countrypcode.split(":")
        if pcode not in adminone.pcodes:
            logger.error(f"PCode {pcode} in {countryiso3} does not exist!")
        else:
            orgcount[pcode] = len(orgdict[countrypcode])
    logger.info("Processed 3W")
    return (
        [["OrgCountAdm1"], ["#org+count+num"]],
        [orgcount],
        [("#org+count+num", today_str, "OCHA", threew_url)],
    )
