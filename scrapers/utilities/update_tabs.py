import logging

from hdx.location.country import Country

logger = logging.getLogger(__name__)

world = (list(), list())
regional = (("regionnames",), ("#region+name",))
national_headers = (
    ("iso3", "countryname", "ishrp", "region"),
    ("#country+code", "#country+name", "#meta+ishrp", "#region+name"),
)
subnational = (
    ("iso3", "countryname", "adm1_pcode", "adm1_name"),
    ("#country+code", "#country+name", "#adm1+code", "#adm1+name"),
)


def update_tab(outputs, name, data):
    logger.info(f"Updating tab: {name}")
    for output in outputs.values():
        output.update_tab(name, data)


def update_world(runner, outputs):
    rows = runner.get_rows("global", ("global",))
    update_tab(outputs, "world", rows)


def update_regional(runner, outputs, regions):
    adms = region.regions + ["global"],
    rows = runner.get_rows("regional", )
    update_tab(outputs, "world", rows)


def update_national(runner, region, hrp_countries, gho_countries, outputs):
    name_fn = lambda adm: Country.get_country_name_from_iso3(adm)
    ishrp_fn = lambda adm: "Y" if adm in hrp_countries else "N"

    def region_fn(adm):
        regions = sorted(list(region.iso3_to_region_and_hrp[adm]))
        regions.remove("GHO")
        return "|".join(regions)

    fns = (lambda x: x, name_fn, ishrp_fn, region_fn)
    rows = runner.get_rows("national", gho_countries, national_headers, fns)
    update_tab(outputs, "national", rows)
