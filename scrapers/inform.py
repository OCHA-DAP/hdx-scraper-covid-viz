import inspect
import logging

from dateutil.relativedelta import relativedelta
from hdx.scraper.readers import read_hdx_metadata
from hdx.utilities.dateparse import default_date, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import Download

logger = logging.getLogger(__name__)


def download_data(date, base_url, countryiso3s, input_cols, downloader):
    url = base_url % date.strftime("%b%Y")
    countries_index = dict()
    while url:
        r = downloader.download(url)
        json = r.json()
        for result in json["results"]:
            countryiso3 = result["iso3"]
            if len(countryiso3) != 1:
                continue
            countryiso3 = countryiso3[0]
            if countryiso3 not in countryiso3s:
                continue
            if result["country_level"] != "Yes":
                continue
            first_val = result[input_cols[0]]
            if not first_val:
                continue
            country_index = countries_index.get(countryiso3, dict())
            individual_or_aggregated = result["individual_aggregated"]
            type_of_crisis = result["type_of_crisis"]
            ind_agg_type = country_index.get("ind_agg_type", dict())
            dict_of_lists_add(ind_agg_type, individual_or_aggregated, type_of_crisis)
            country_index["ind_agg_type"] = ind_agg_type
            crises_index = country_index.get("crises", dict())
            crisis_index = crises_index.get(type_of_crisis, dict())
            last_updated = result["Last updated"]
            for input_col in input_cols:
                crisis_index[input_col] = (result[input_col], last_updated)
            crises_index[type_of_crisis] = crisis_index
            country_index["crises"] = crises_index
            countries_index[countryiso3] = country_index
        url = json["next"]
    return countries_index


def get_columns_by_date(
    date, base_url, countryiso3s, input_col, downloader, crisis_types, not_found
):
    countries_index = download_data(
        date, base_url, countryiso3s, [input_col], downloader
    )
    valuedict = dict()
    for countryiso3, type_of_crisis in crisis_types.items():
        country_index = countries_index.get(countryiso3)
        if not country_index:
            not_found.add(countryiso3)
            continue
        crisis = country_index["crises"].get(type_of_crisis)
        if not crisis:
            not_found.add(countryiso3)
            continue
        val, last_updated = crisis[input_col]
        valuedict[countryiso3] = val
    return valuedict


def get_latest_columns(date, base_url, countryiso3s, input_cols, downloader):
    countries_index = download_data(
        date, base_url, countryiso3s, input_cols, downloader
    )
    valuedicts = [dict() for _ in input_cols]
    crisis_types = dict()
    max_date = default_date
    for countryiso3, country_data in countries_index.items():
        crises_types = country_data["ind_agg_type"].get("Aggregated")
        if not crises_types:
            crises_types = country_data["ind_agg_type"].get("Individual")
        type_of_crisis = crises_types[0]
        crisis_types[countryiso3] = type_of_crisis
        crisis = country_data["crises"][type_of_crisis]
        for i, input_col in enumerate(input_cols):
            val, last_updated = crisis[input_col]
            valuedicts[i][countryiso3] = val
            date = parse_date(last_updated)
            if date > max_date:
                max_date = date
    return valuedicts, crisis_types, max_date


def get_inform(configuration, today, countryiso3s, other_auths, scrapers=None):
    name = inspect.currentframe().f_code.co_name
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list()
    inform_configuration = configuration["inform"]
    read_hdx_metadata(inform_configuration)
    input_cols = inform_configuration["input_cols"]
    trend_input_col = inform_configuration["trend_input_col"]
    base_url = inform_configuration["url"]
    with Download(
        rate_limit={"calls": 1, "period": 0.1},
        headers={"Authorization": other_auths["inform"]},
    ) as downloader:
        start_date = today - relativedelta(months=1)
        valuedictsfortoday, crisis_types, max_date = get_latest_columns(
            start_date, base_url, countryiso3s, input_cols, downloader
        )
        severity_indices = [valuedictsfortoday[0]]
        not_found = set()
        for i in range(1, 6, 1):
            prevdate = start_date - relativedelta(months=i)
            valuedictfordate = get_columns_by_date(
                prevdate,
                base_url,
                countryiso3s,
                trend_input_col,
                downloader,
                crisis_types,
                not_found,
            )
            severity_indices.append(valuedictfordate)

    trend_valuedict = dict()
    valuedictsfortoday.append(trend_valuedict)
    for countryiso3 in severity_indices[0]:
        if countryiso3 in not_found:
            trend_valuedict[countryiso3] = "-"
            continue
        avg = round(
            (
                severity_indices[0][countryiso3]
                + severity_indices[1][countryiso3]
                + severity_indices[2][countryiso3]
            )
            / 3,
            1,
        )
        prevavg = round(
            (
                severity_indices[3][countryiso3]
                + severity_indices[4][countryiso3]
                + severity_indices[5][countryiso3]
            )
            / 3,
            1,
        )
        if avg == prevavg:
            trend = "stable"
        elif avg < prevavg:
            trend = "decreasing"
        else:
            trend = "increasing"
        trend_valuedict[countryiso3] = trend
    logger.info("Processed INFORM")
    source_date = max_date.date().isoformat()
    output_cols = inform_configuration["output_cols"] + [
        inform_configuration["trend_output_col"]
    ]
    hxltags = inform_configuration["output_hxltags"] + [
        inform_configuration["trend_hxltag"]
    ]
    return (
        [output_cols, hxltags],
        valuedictsfortoday,
        [
            (
                hxltag,
                source_date,
                inform_configuration["source"],
                inform_configuration["source_url"],
            )
            for hxltag in hxltags
        ],
    )
