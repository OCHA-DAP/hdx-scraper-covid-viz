import logging
from os.path import join

from hdx.location.adminone import AdminOne
from hdx.location.country import Country
from hdx.scraper.configurable.aggregator import Aggregator
from hdx.scraper.outputs.update_tabs import (
    get_regional_rows,
    get_toplevel_rows,
    update_national,
    update_regional,
    update_sources,
    update_subnational,
    update_toplevel,
)
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.fallbacks import Fallbacks

from .covax_deliveries import CovaxDeliveries
from .education_closures import EducationClosures
from .education_enrolment import EducationEnrolment
from .food_prices import FoodPrices
from .fts import FTS
from .inform import Inform
from .iom_dtm import IOMDTM
from .ipc import IPC
from .report import get_report_source
from .unhcr import UNHCR
from .unhcr_myanmar_idps import idps_post_run
from .utilities.region_lookups import RegionLookups
from .vaccination_campaigns import VaccinationCampaigns
from .who_covid import WHOCovid
from .whowhatwhere import WhoWhatWhere

logger = logging.getLogger(__name__)


def get_indicators(
    configuration,
    today,
    outputs,
    tabs,
    scrapers_to_run=None,
    gho_countries_override=None,
    hrp_countries_override=None,
    errors_on_exit=None,
    use_live=True,
    fallbacks_root="",
):
    Country.countriesdata(
        use_live=use_live,
        country_name_overrides=configuration["country_name_overrides"],
        country_name_mappings=configuration["country_name_mappings"],
    )

    if gho_countries_override:
        gho_countries = gho_countries_override
    else:
        gho_countries = configuration["gho"]
    if hrp_countries_override:
        hrp_countries = hrp_countries_override
    else:
        hrp_countries = configuration["HRPs"]
    configuration["countries_fuzzy_try"] = hrp_countries
    adminone = AdminOne(configuration)
    regional_configuration = configuration["regional"]
    RegionLookups.load(regional_configuration, gho_countries, hrp_countries)
    if fallbacks_root is not None:
        fallbacks_path = join(fallbacks_root, configuration["json"]["output"])
        levels_mapping = {
            "global": "world_data",
            "regional": "regional_data",
            "national": "national_data",
            "subnational": "subnational_data",
        }
        Fallbacks.add(
            fallbacks_path,
            levels_mapping=levels_mapping,
            sources_key="sources_data",
        )
    runner = Runner(
        gho_countries,
        adminone,
        today,
        errors_on_exit=errors_on_exit,
        scrapers_to_run=scrapers_to_run,
    )
    configurable_scrapers = dict()
    for level_name in "national", "subnational", "global":
        if level_name == "global":
            level = "single"
        else:
            level = level_name
        suffix = f"_{level_name}"
        configurable_scrapers[level_name] = runner.add_configurables(
            configuration[f"scraper{suffix}"], level, level_name, suffix=suffix
        )
    runner.add_instance_variables(
        "idps_national", overrideinfo=configuration["unhcr_myanmar_idps"]
    )
    runner.add_post_run("idps_national", idps_post_run)

    who_covid = WHOCovid(
        configuration["who_covid"],
        outputs,
        hrp_countries,
        gho_countries,
        RegionLookups.gho_iso3_to_region,
    )
    ipc = IPC(configuration["ipc"], today, gho_countries, adminone)

    fts = FTS(configuration["fts"], today, outputs, gho_countries)
    food_prices = FoodPrices(configuration["food_prices"], today, gho_countries)
    vaccination_campaigns = VaccinationCampaigns(
        configuration["vaccination_campaigns"],
        gho_countries,
        outputs,
    )
    unhcr = UNHCR(configuration["unhcr"], today, gho_countries)
    inform = Inform(configuration["inform"], today, gho_countries)
    covax_deliveries = CovaxDeliveries(configuration["covax_deliveries"], gho_countries)
    education_closures = EducationClosures(
        configuration["education_closures"],
        today,
        gho_countries,
        RegionLookups.gho_iso3_to_region,
    )
    education_enrolment = EducationEnrolment(
        configuration["education_enrolment"],
        education_closures,
        gho_countries,
        RegionLookups.gho_iso3_to_region,
    )
    national_names = configurable_scrapers["national"] + [
        "food_prices",
        "vaccination_campaigns",
        "fts",
        "unhcr",
        "inform",
        "ipc",
        "covax_deliveries",
        "education_closures",
        "education_enrolment",
    ]
    national_names.insert(1, "who_covid")

    whowhatwhere = WhoWhatWhere(configuration["whowhatwhere"], today, adminone)
    iomdtm = IOMDTM(configuration["iom_dtm"], today, adminone)
    global_names = ["who_covid", "fts"] + configurable_scrapers["global"]

    subnational_names = configurable_scrapers["subnational"] + [
        "whowhatwhere",
        "iom_dtm",
    ]
    subnational_names.insert(1, "ipc")

    runner.add_customs(
        (
            who_covid,
            ipc,
            fts,
            food_prices,
            vaccination_campaigns,
            unhcr,
            inform,
            covax_deliveries,
            education_closures,
            education_enrolment,
            whowhatwhere,
            iomdtm,
        )
    )
    regional_scrapers_gho = Aggregator.get_scrapers(
        regional_configuration["aggregate_gho"],
        "national",
        "regional",
        RegionLookups.gho_iso3_to_region,
        runner,
    )
    regional_names_gho = runner.add_customs(regional_scrapers_gho, add_to_run=True)
    regional_scrapers_hrp = Aggregator.get_scrapers(
        regional_configuration["aggregate_hrp"],
        "national",
        "regional",
        RegionLookups.hrp_iso3_to_region,
        runner,
    )
    regional_names_hrp = runner.add_customs(regional_scrapers_hrp, add_to_run=True)
    runner.run(
        prioritise_scrapers=(
            "population_national",
            "population_subnational",
            "population_regional",
        )
    )

    regional_names = list()
    for name in regional_names_gho:
        if name == "affected_children_sam_regional":
            regional_names.extend(regional_names_hrp)
        regional_names.append(name)
    regional_names.extend(["education_closures", "education_enrolment"])
    regional_rows = get_regional_rows(
        runner,
        RegionLookups.regions + ["global"],
        names=regional_names,
    )

    if "national" in tabs:
        flag_countries = {
            "header": "ishrp",
            "hxltag": "#meta+ishrp",
            "countries": hrp_countries,
        }
        update_national(
            runner,
            gho_countries,
            outputs,
            names=national_names,
            flag_countries=flag_countries,
            iso3_to_region=RegionLookups.gho_iso3_to_region,
            ignore_regions=("GHO",),
        )
    if "regional" in tabs:
        global_rows = get_toplevel_rows(
            runner, overrides={"who_covid": {"gho": "global"}}, toplevel="global"
        )
        additional_global_headers = (
            "Cumulative_cases",
            "Cumulative_deaths",
            "RequiredFunding",
            "Funding",
            "PercentFunded",
        )
        update_regional(
            outputs,
            regional_rows,
            toplevel_rows=global_rows,
            additional_toplevel_headers=additional_global_headers,
            toplevel="global",
        )
    if "world" in tabs:
        global_rows = get_toplevel_rows(
            runner,
            names=global_names,
            overrides={"who_covid": {"gho": "global"}},
            toplevel="global",
        )
        update_toplevel(
            outputs,
            global_rows,
            tab="world",
            regional_rows=regional_rows,
            regional_adm="GHO",
            regional_hxltags=configuration["regional"]["global"],
        )
    if "subnational" in tabs:
        update_subnational(runner, adminone, outputs, names=subnational_names)

    adminone.output_matches()
    adminone.output_ignored()
    adminone.output_errors()

    names = national_names
    for name in global_names:
        if name not in names:
            names.append(name)
    for name in subnational_names:
        if name not in names:
            names.append(name)
    if "sources" in tabs:
        update_sources(
            runner,
            configuration,
            outputs,
            names=names,
            additional_sources=(get_report_source(configuration),),
        )
    return hrp_countries
