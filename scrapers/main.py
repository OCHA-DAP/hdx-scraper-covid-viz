import logging

from hdx.location.adminone import AdminOne
from hdx.location.country import Country
from hdx.scraper.runner import Runner
from scrapers.utilities.region_aggregation import RegionAggregation
from scrapers.utilities.update_tabs import (
    update_national,
    update_regional,
    update_sources,
    update_subnational,
    update_world,
)

from .covax_deliveries import CovaxDeliveries
from .education_closures import EducationClosures
from .education_enrolment import EducationEnrolment
from .food_prices import FoodPrices
from .fts import FTS
from .inform import Inform
from .iom_dtm import IOMDTM
from .ipc_old import IPC
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
    retriever,
    outputs,
    tabs,
    scrapers_to_run=None,
    basic_auths=dict(),
    other_auths=dict(),
    countries_override=None,
    errors_on_exit=None,
    use_live=True,
):
    Country.countriesdata(
        use_live=use_live,
        country_name_overrides=configuration["country_name_overrides"],
        country_name_mappings=configuration["country_name_mappings"],
    )

    if countries_override:
        gho_countries = countries_override
        hrp_countries = countries_override
    else:
        gho_countries = configuration["gho"]
        hrp_countries = configuration["HRPs"]
    configuration["countries_fuzzy_try"] = hrp_countries
    downloader = retriever.downloader
    adminone = AdminOne(configuration)
    RegionLookups.load(
        configuration["regional"], today, downloader, gho_countries, hrp_countries
    )
    if scrapers_to_run is not None:
        scrapers_to_run = ["population"] + scrapers_to_run + ["region_aggregation"]
    runner = Runner(
        gho_countries,
        adminone,
        downloader,
        basic_auths,
        today,
        errors_on_exit=errors_on_exit,
        scrapers_to_run=scrapers_to_run,
    )
    configurable_scrapers = dict()
    for level in "national", "subnational", "global":
        suffix = f"_{level}"
        configurable_scrapers[level] = runner.add_configurables(
            configuration[f"scraper{suffix}"], level, suffix=suffix
        )
    runner.add_instance_variables(
        "idps_national", overrideinfo=configuration["unhcr_myanmar_idps"]
    )
    runner.add_post_run("idps_national", idps_post_run)

    who_covid = WHOCovid(
        configuration["who_covid"],
        today,
        outputs,
        hrp_countries,
        gho_countries,
        RegionLookups.iso3_to_region,
    )
    ipc = IPC(configuration["ipc"], today, gho_countries, adminone, downloader)

    fts = FTS(configuration["fts"], today, gho_countries, basic_auths)
    food_prices = FoodPrices(
        configuration["food_prices"], today, gho_countries, retriever, basic_auths
    )
    vaccination_campaigns = VaccinationCampaigns(
        configuration["vaccination_campaigns"],
        today,
        gho_countries,
        downloader,
        outputs,
    )
    unhcr = UNHCR(configuration["unhcr"], today, gho_countries, downloader)
    inform = Inform(configuration["inform"], today, gho_countries, other_auths)
    covax_deliveries = CovaxDeliveries(
        configuration["covax_deliveries"], today, gho_countries, downloader
    )
    education_closures = EducationClosures(
        configuration["education_closures"],
        today,
        gho_countries,
        RegionLookups.iso3_to_region_and_hrp,
        downloader,
    )
    education_enrolment = EducationEnrolment(
        configuration["education_enrolment"],
        education_closures,
        gho_countries,
        RegionLookups.iso3_to_region_and_hrp,
        downloader,
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

    whowhatwhere = WhoWhatWhere(
        configuration["whowhatwhere"], today, adminone, downloader
    )
    iomdtm = IOMDTM(configuration["iom_dtm"], today, adminone, downloader)
    global_names = ["who_covid", "fts"] + configurable_scrapers["global"]

    subnational_names = configurable_scrapers["subnational"] + [
        "whowhatwhere",
        "iomdtm",
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
    regional_scrapers = RegionAggregation.get_regional_scrapers(
        configuration["regional"],
        hrp_countries,
        RegionLookups.iso3_to_region_and_hrp,
        runner,
    )
    runner.add_customs(regional_scrapers)
    runner.run(
        prioritise_scrapers=(
            "population_national",
            "population_subnational",
            "population_regional",
        )
    )

    if "world" in tabs:
        update_world(runner, global_names, outputs, {"who_covid": {"global": "gho"}})
    if "regional" in tabs:
        update_regional(runner, RegionLookups.regions + ["global"], outputs)
    if "national" in tabs:
        update_national(
            runner,
            national_names,
            RegionLookups.iso3_to_region_and_hrp,
            hrp_countries,
            gho_countries,
            outputs,
        )
    if "subnational" in tabs:
        update_subnational(runner, subnational_names, adminone, outputs)

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
        update_sources(runner, names, configuration, today, outputs)
    return hrp_countries
