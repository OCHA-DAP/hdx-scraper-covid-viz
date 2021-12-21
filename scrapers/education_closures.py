import logging

from hdx.scraper.readers import read
from hdx.scraper.scrapers import use_fallbacks
from hdx.utilities.dateparse import default_date, parse_date
from hdx.utilities.downloader import DownloadError

logger = logging.getLogger(__name__)


def get_education_closures(
    configuration, today, countryiso3s, regionlookup, downloader, fallbacks=None, scrapers=None
):
    name = "education_closures"
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list(), list(), list(), list()
    datasetinfo = configuration[name]
    output_cols = ["School Closure"]
    output_hxltags = ["#impact+type"]
    r_output_cols = ["No. closed countries"]
    r_output_hxltags = ["#status+country+closed"]
    try:
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
        headers = [output_cols, output_hxltags]

    except DownloadError:
        results = dict()
        use_fallbacks(name, fallbacks["national"], output_cols, output_hxltags, results)

    logger.info("Processed education closures")
    return (
        [["No. closed countries"], ["#status+country+closed"]],
        [closed_countries],
        [
            (
                "#status+country+closed",
                datasetinfo["date"],
                datasetinfo["source"],
                datasetinfo["source_url"],
            )
        ],
        [["School Closure"], ["#impact+type"]],
        [closures],
        [
            (
                "#impact+type",
                datasetinfo["date"],
                datasetinfo["source"],
                datasetinfo["source_url"],
            )
        ],
        fully_closed,
    )
