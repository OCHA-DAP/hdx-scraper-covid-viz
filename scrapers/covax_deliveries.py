import logging

from hdx.scraper.readers import read
from hdx.scraper.utils import get_sources_from_datasetinfo
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import get_numeric_if_possible

logger = logging.getLogger(__name__)


def get_covax_deliveries(configuration, today, countryiso3s, downloader, scrapers=None):
    name = "covax_deliveries"
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list()
    datasetinfo = configuration[name]
    headers, iterator = read(downloader, datasetinfo, today=today)
    hxlrow = next(iterator)
    doses_lookup = dict()
    for row in iterator:
        newrow = dict()
        for key in row:
            newrow[hxlrow[key]] = row[key]
        countryiso = newrow["#country+code"]
        if not countryiso or countryiso not in countryiso3s:
            continue
        key = f'{countryiso}|{newrow["#meta+vaccine+pipeline"]}|{newrow["#meta+vaccine+producer"]}|{newrow["#meta+vaccine+funder"]}'
        nodoses = get_numeric_if_possible(newrow["#capacity+vaccine+doses"])
        if nodoses:
            doses_lookup[key] = doses_lookup.get(key, 0) + nodoses
    funders = dict()
    producers = dict()
    doses = dict()
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
    hxltags = [
        "#meta+vaccine+producer",
        "#meta+vaccine+funder",
        "#capacity+vaccine+doses",
    ]
    return (
        [["Vaccine", "Funder", "Doses"], hxltags],
        [producers, funders, doses],
        get_sources_from_datasetinfo(datasetinfo, hxltags),
    )
