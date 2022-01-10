import logging

from hdx.scraper.readers import read
from hdx.scraper.scrapers import use_fallbacks
from hdx.utilities.dateparse import default_date, parse_date
from hdx.utilities.downloader import DownloadError

from scrapers.utils import add_to_results

logger = logging.getLogger(__name__)

headers = {"national": [["School Closure"], ["#impact+type"]],
           "regional": [["No. closed countries"], ["#status+country+closed"]]}


def _get_fully_closed(closures):
    fully_closed = list()
    for countryiso, closure in closures.items():
        if closure.lower() == "closed due to covid-19":
            fully_closed.append(countryiso)
    return fully_closed


def _get_education_closures(
    datasetinfo, today, countryiso3s, regionlookup, downloader, results
):
    closures_headers, closures_iterator = read(downloader, datasetinfo)
    values = {"national": [dict()], "regional": [dict()]}
    closures = values["national"][0]
    closed_countries = values["regional"][0]
    country_dates = dict()
    for row in closures_iterator:
        countryiso = row["ISO"]
        if not countryiso or countryiso not in countryiso3s:
            continue
        date = row["Date"]
        if isinstance(date, str):
            date = parse_date(date)
        if date > today:
            continue
        max_date = country_dates.get(countryiso, default_date)
        if date < max_date:
            continue
        country_dates[countryiso] = date
        closures[countryiso] = row["Status"]
    fully_closed = _get_fully_closed(closures)
    for countryiso in values:
        for region in regionlookup.iso3_to_region_and_hrp[countryiso]:
            if countryiso in fully_closed:
                closed_countries[region] = closed_countries.get(region, 0) + 1

    add_to_results(headers, values, datasetinfo, results)
    return fully_closed


def get_education_closures(
    configuration,
    today,
    countryiso3s,
    regionlookup,
    downloader,
    fallbacks=None,
    scrapers=None,
):
    name = "education_closures"
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list(), list(), list(), list(), list()
    datasetinfo = configuration[name]
    results = {"national": dict(), "regional": dict()}
    try:
        fully_closed = _get_education_closures(
            datasetinfo, today, countryiso3s, regionlookup, downloader, results
        )
    except DownloadError:
        for level in results:
            use_fallbacks(
                name,
                fallbacks[level],
                headers[level][0],
                headers[level][1],
                results[level],
            )
        fully_closed = _get_fully_closed(results["national"]["values"][0])

    logger.info("Processed education closures")
    return (
        results,
        fully_closed,
    )
