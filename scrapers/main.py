import logging
from os.path import join

from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.scraper.framework.runner import Runner
from hdx.scraper.framework.utilities.fallbacks import Fallbacks
from hdx.scraper.framework.utilities.region_lookup import RegionLookup
from hdx.scraper.framework.utilities.sources import Sources
from hdx.scraper.framework.utilities.writer import Writer

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
    adminlevel = AdminLevel(configuration)
    adminlevel.setup_from_admin_info(configuration["admin_info"])
    regional_configuration = configuration["regional"]
    RegionLookup.load(regional_configuration, gho_countries, {"HRPs": hrp_countries})
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
    Sources.set_default_source_date_format("%Y-%m-%d")
    runner = Runner(
        gho_countries,
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
            configuration[f"scraper{suffix}"],
            level,
            adminlevel,
            level_name,
            suffix=suffix,
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
        RegionLookup.iso3_to_region,
    )
    ipc = IPC(configuration["ipc"], today, gho_countries, adminlevel)

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
        RegionLookup.iso3_to_regions["GHO"],
    )
    education_enrolment = EducationEnrolment(
        configuration["education_enrolment"],
        education_closures,
        gho_countries,
        RegionLookup.iso3_to_regions["GHO"],
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

    whowhatwhere = WhoWhatWhere(configuration["whowhatwhere"], today, adminlevel)
    iomdtm = IOMDTM(configuration["iom_dtm"], today, adminlevel)
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
    regional_names_gho = runner.add_aggregators(
        True,
        regional_configuration["aggregate_gho"],
        "national",
        "regional",
        RegionLookup.iso3_to_regions["GHO"],
        force_add_to_run=True,
    )
    regional_names_hrp = runner.add_aggregators(
        True,
        regional_configuration["aggregate_hrp"],
        "national",
        "regional",
        RegionLookup.iso3_to_regions["HRPs"],
        force_add_to_run=True,
    )
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

    writer = Writer(runner, outputs)
    regional_rows = writer.get_regional_rows(
        RegionLookup.regions + ["global"],
        names=regional_names,
    )

    if "national" in tabs:
        flag_countries = {
            "header": "ishrp",
            "hxltag": "#meta+ishrp",
            "countries": hrp_countries,
        }
        writer.update_national(
            gho_countries,
            names=national_names,
            flag_countries=flag_countries,
            iso3_to_region=RegionLookup.iso3_to_regions["GHO"],
            ignore_regions=("GHO",),
        )
    if "regional" in tabs:
        global_rows = writer.get_toplevel_rows(
            overrides={"who_covid": {"gho": "global"}}, toplevel="global"
        )
        writer.update_regional(
            regional_rows,
            toplevel_rows=global_rows,
            toplevel_hxltags=configuration["regional"]["regional_from_global"],
            toplevel="global",
        )
    if "world" in tabs:
        global_rows = writer.get_toplevel_rows(
            names=global_names,
            overrides={"who_covid": {"gho": "global"}},
            toplevel="global",
        )
        writer.update_toplevel(
            global_rows,
            tab="world",
            regional_rows=regional_rows,
            regional_adm="GHO",
            regional_hxltags=configuration["regional"]["global_from_regional"],
        )
    if "subnational" in tabs:
        writer.update_subnational(adminlevel, names=subnational_names)

    adminlevel.output_matches()
    adminlevel.output_ignored()
    adminlevel.output_errors()

    names = national_names
    for name in global_names:
        if name not in names:
            names.append(name)
    for name in subnational_names:
        if name not in names:
            names.append(name)
    if "sources" in tabs:
        writer.update_sources(
            additional_sources=configuration["additional_sources"],
            names=names,
            custom_sources=(get_report_source(configuration),),
        )
    return hrp_countries
