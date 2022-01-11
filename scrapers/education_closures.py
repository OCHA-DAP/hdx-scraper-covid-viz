import logging

from hdx.scraper.readers import read
from hdx.scraper.utils import get_sources_from_datasetinfo
from hdx.utilities.dateparse import default_date, parse_date

logger = logging.getLogger(__name__)


def get_education_closures(
    configuration, today, countryiso3s, regionlookup, downloader, scrapers=None
):
    name = "education_closures"
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list(), list(), list(), list(), list()
    datasetinfo = configuration[name]
    closures_headers, closures_iterator = read(downloader, datasetinfo)
    closures = dict()
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
    fully_closed = list()
    for countryiso, closure in closures.items():
        if closure.lower() == "closed due to covid-19":
            fully_closed.append(countryiso)
    closed_countries = dict()
    for countryiso in closures:
        for region in regionlookup.iso3_to_region_and_hrp[countryiso]:
            if countryiso in fully_closed:
                closed_countries[region] = closed_countries.get(region, 0) + 1
    rhxltags = ["#status+country+closed"]
    hxltags = ["#impact+type"]
    logger.info("Processed education closures")
    return (
        [["No. closed countries"], rhxltags],
        [closed_countries],
        get_sources_from_datasetinfo(datasetinfo, rhxltags),
        [["School Closure"], hxltags],
        [closures],
        get_sources_from_datasetinfo(datasetinfo, hxltags),
        fully_closed,
    )
