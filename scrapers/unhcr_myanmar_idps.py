import inspect
import logging

logger = logging.getLogger(__name__)


def patch_unhcr_myanmar_idps(configuration, national, downloader, scrapers=None):
    name = inspect.currentframe().f_code.co_name
    if scrapers and not any(scraper in name for scraper in scrapers):
        return
    downloader.download(configuration["unhcr_myanmar_idps"]["url"])
    number_idps = int(downloader.get_json()["data"][0]["individuals"])
    index = national[1].index("#affected+displaced")
    for i, row in enumerate(national[2:]):
        if row[0] != "MMR":
            continue
        current_idps = national[i + 2][index]
        logger.info(f"Replacing {current_idps} with {number_idps} for MMR IDPs!")
        national[i + 2][index] = number_idps
    logger.info("Processed UNHCR Myanmar IDPs")
