import logging

from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.scraper.utilities import get_isodate_from_dataset_date

from ..monthly_report import get_monthly_report_source

logger = logging.getLogger(__name__)

regional_headers = (("regionnames",), ("#region+name",))
national_headers = (
    ("iso3", "countryname", "ishrp", "region"),
    ("#country+code", "#country+name", "#meta+ishrp", "#region+name"),
)
subnational_headers = (
    ("iso3", "countryname", "adm1_pcode", "adm1_name"),
    ("#country+code", "#country+name", "#adm1+code", "#adm1+name"),
)
sources_headers = (
    ("Indicator", "Date", "Source", "Url"),
    ("#indicator+name", "#date", "#meta+source", "#meta+url"),
)


def update_tab(outputs, name, data):
    logger.info(f"Updating tab: {name}")
    for output in outputs.values():
        output.update_tab(name, data)


def get_global_rows(runner, names, overrides=dict()):
    return runner.get_rows("global", ("value",), names=names, overrides=overrides)


def get_regional_rows(runner, names, regions):
    return runner.get_rows(
        "regional", regions, regional_headers, (lambda adm: adm,), names=names
    )


def update_world(outputs, global_rows, regional_rows=tuple(), gho_to_world=tuple()):
    if not global_rows:
        return
    if regional_rows:
        adm_header = regional_rows[1].index("#region+name")
        for row in regional_rows[2:]:
            if row[adm_header] == "GHO":
                for i, header in enumerate(regional_rows[0]):
                    if header in gho_to_world:
                        global_rows[0].append(header)
                        global_rows[1].append(regional_rows[1][i])
                        global_rows[2].append(row[i])
    update_tab(outputs, "world", global_rows)


def update_regional(
    outputs, regional_rows, global_rows=tuple(), additional_global_headers=tuple()
):
    if not regional_rows:
        return
    global_values = dict()
    if global_rows:
        for i, header in enumerate(global_rows[0]):
            if header in additional_global_headers:
                global_values[header] = global_rows[2][i]
    adm_header = regional_rows[1].index("#region+name")
    for row in regional_rows[2:]:
        if row[adm_header] == "global":
            for i, header in enumerate(regional_rows[0]):
                value = global_values.get(header)
                if value is None:
                    continue
                row[i] = value
    update_tab(outputs, "regional", regional_rows)


def update_national(
    runner, names, iso3_to_region_and_hrp, hrp_countries, gho_countries, outputs
):
    name_fn = lambda adm: Country.get_country_name_from_iso3(adm)
    ishrp_fn = lambda adm: "Y" if adm in hrp_countries else "N"

    def region_fn(adm):
        regions = sorted(list(iso3_to_region_and_hrp[adm]))
        regions.remove("GHO")
        return "|".join(regions)

    fns = (lambda adm: adm, name_fn, ishrp_fn, region_fn)
    rows = runner.get_rows(
        "national", gho_countries, national_headers, fns, names=names
    )
    if rows:
        update_tab(outputs, "national", rows)


def update_subnational(runner, names, adminone, outputs):
    def get_country_name(adm):
        countryiso3 = adminone.pcode_to_iso3[adm]
        return Country.get_country_name_from_iso3(countryiso3)

    fns = (
        lambda adm: adminone.pcode_to_iso3[adm],
        get_country_name,
        lambda adm: adm,
        lambda adm: adminone.pcode_to_name[adm],
    )
    rows = runner.get_rows(
        "subnational", adminone.pcodes, subnational_headers, fns, names=names
    )
    if rows:
        update_tab(outputs, "subnational", rows)


def update_sources(runner, names, configuration, outputs):
    sources = runner.get_sources(
        names, additional_sources=configuration["additional_sources"]
    )
    sources.append(get_monthly_report_source(configuration))
    update_tab(outputs, "sources", list(sources_headers) + sources)
