import logging

from hdx.data.dataset import Dataset
from hdx.location.adminone import AdminOne
from hdx.location.country import Country
from hdx.scraper.runner import Runner
from hdx.scraper.utilities import get_isodate_from_dataset_date

from scrapers.utilities.update_tabs import update_national, update_world, \
    update_regional
from scrapers.utilities.region_aggregation import RegionAggregation

from .covax_deliveries import CovaxDeliveries
from .education_closures import EducationClosures
from .education_enrolment import EducationEnrolment
from .food_prices import FoodPrices
from .fts import FTS
from .inform import Inform
from .iom_dtm import IOMDTM
from .ipc_old import IPC
from .monthly_report import get_monthly_report_source
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
    sources = [
        ("Indicator", "Date", "Source", "Url"),
        ("#indicator+name", "#date", "#meta+source", "#meta+url"),
    ]

    def update_tab(name, data):
        logger.info(f"Updating tab: {name}")
        for output in outputs.values():
            output.update_tab(name, data)

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
    RegionLookups.load(configuration["regional"], today, downloader, gho_countries, hrp_countries)
    population_lookup = dict()
    if scrapers_to_run is not None:
        scrapers_to_run = ["population"] + scrapers_to_run + ["region"]
    runner = Runner(
        gho_countries,
        adminone,
        downloader,
        basic_auths,
        today,
        errors_on_exit=errors_on_exit,
        scrapers_to_run=scrapers_to_run
    )
    configurable_scrapers = dict()
    for level in "national", "subnational", "global":
        suffix = f"_{level}"
        configurable_scrapers[level] = runner.add_configurables(configuration[f"scraper{suffix}"], level, suffix=suffix)
    runner.add_instance_variables("idps_national",
                                  overrideinfo=configuration["unhcr_myanmar_idps"])
    runner.add_post_run("idps_national", idps_post_run)

    who_covid = WHOCovid(configuration["who_covid"],
        today, outputs, hrp_countries, gho_countries, RegionLookups.iso3_to_region, population_lookup
    )    
    ipc = IPC(configuration["ipc"], today, gho_countries, adminone, downloader)
    
    fts = FTS(configuration["fts"], today, gho_countries, basic_auths)
    food_prices = FoodPrices(configuration["food_prices"], today, gho_countries, retriever, basic_auths)
    vaccination_campaigns = VaccinationCampaigns(
        configuration["vaccination_campaigns"], today, gho_countries, downloader, outputs
    )
    unhcr = UNHCR(configuration["unhcr"], today, gho_countries, downloader)
    inform = Inform(configuration["inform"], today, gho_countries, other_auths)
    covax_deliveries = CovaxDeliveries(configuration["covax_deliveries"], today, gho_countries, downloader)
    education_closures = EducationClosures(configuration["education_closures"], today, gho_countries, RegionLookups.iso3_to_region_and_hrp, downloader)
    education_enrolment = EducationEnrolment(configuration["education_enrolment"],
        education_closures, gho_countries, RegionLookups.iso3_to_region_and_hrp, downloader
    )

    # national_headers = extend_headers(
    #     national,
    #     who_results["national"]["headers"],
    #     generic_national_results["headers"],
    #     food_results["national"]["headers"],
    #     campaigns_results["national"]["headers"],
    #     fts_results["national"]["headers"],
    #     unhcr_results["national"]["headers"],
    #     inform_results["national"]["headers"],
    #     ipc_results["national"]["headers"],
    #     covax_results["national"]["headers"],
    #     closures_results["national"]["headers"],
    #     enrolment_results["national"]["headers"],
    # )
    # national_columns = extend_columns(
    #     "national",
    #     national,
    #     gho_countries,
    #     hrp_countries,
    #     region,
    #     None,
    #     national_headers,
    #     who_results["national"]["values"],
    #     generic_national_results["values"],
    #     food_results["national"]["values"],
    #     campaigns_results["national"]["values"],
    #     fts_results["national"]["values"],
    #     unhcr_results["national"]["values"],
    #     inform_results["national"]["values"],
    #     ipc_results["national"]["values"],
    #     covax_results["national"]["values"],
    #     closures_results["national"]["values"],
    #     enrolment_results["national"]["values"],
    # )
    # extend_sources(
    #     sources,
    #     who_results["national"]["sources"],
    #     generic_national_results["sources"],
    #     food_results["national"]["sources"],
    #     campaigns_results["national"]["sources"],
    #     fts_results["national"]["sources"],
    #     unhcr_results["national"]["sources"],
    #     inform_results["national"]["sources"],
    #     ipc_results["national"]["sources"],
    #     covax_results["national"]["sources"],
    #     closures_results["national"]["sources"],
    #     enrolment_results["national"]["sources"],
    # )
    # patch_unhcr_myanmar_idps(
    #     configuration, national, downloader, runner, scrapers=scrapers_to_run
    # )
    # update_tab("national", national)
    national_names = ["who_covid"] + configurable_scrapers["national"] + ["food_prices", "vaccination_campaigns", "fts", "unhcr", "inform", "ipc", "covax_deliveries", "education_closures", "education_enrolment"]

    # regional_headers, regional_columns = region.get_regional(
    #     region,
    #     national_headers,
    #     national_columns,
    #     None,
    #     (who_results["global"]["headers"], who_results["global"]["values"]),
    #     (fts_results["global"]["headers"], fts_results["global"]["values"]),
    # )
    # regional_headers = extend_headers(
    #     regional,
    #     regional_headers,
    #     closures_results["regional"]["headers"],
    #     enrolment_results["regional"]["headers"],
    # )
    # regional_columns = extend_columns(
    #     "regional",
    #     regional,
    #     region.regions + ["global"],
    #     None,
    #     region,
    #     None,
    #     regional_headers,
    #     regional_columns,
    #     closures_results["regional"]["values"],
    #     enrolment_results["regional"]["values"],
    # )
    #     update_tab("regional", regional)
    #     extend_sources(
    #         sources,
    #         closures_results["regional"]["sources"],
    #         enrolment_results["regional"]["sources"],
    #     )
    # rgheaders, rgcolumns = region.get_world(
    #     regional_headers, regional_columns
    # )
    # results = runner.run_generic_scrapers("global", scrapers_to_run)
    # world_headers = extend_headers(
    #     world,
    #     who_results["gho"]["headers"],
    #     fts_results["global"]["headers"],
    #     results["headers"],
    #     rgheaders,
    # )
    # extend_columns(
    #     "global",
    #     world,
    #     None,
    #     None,
    #     None,
    #     None,
    #     world_headers,
    #     who_results["gho"]["values"],
    #     fts_results["global"]["values"],
    #     results["values"],
    #     rgcolumns,
    # )
    # extend_sources(
    #     sources, fts_results["global"]["sources"], results["sources"]
    # )
    # update_tab("world", world)

    whowhatwhere = WhoWhatWhere(configuration["whowhatwhere"], today, adminone, downloader)
    iomdtm = IOMDTM(configuration["iom_dtm"], today, adminone, downloader)
    global_names = ["who_covid", "fts"] + configurable_scrapers["global"]

    # subnational_headers = extend_headers(
    #     subnational,
    #     ipc_results["subnational"]["headers"],
    #     results["headers"],
    #     whowhatwhere_results["subnational"]["headers"],
    #     iomdtm_results["subnational"]["headers"],
    # )
    # extend_columns(
    #     "subnational",
    #     subnational,
    #     pcodes,
    #     None,
    #     None,
    #     adminone,
    #     subnational_headers,
    #     ipc_results["subnational"]["values"],
    #     results["values"],
    #     whowhatwhere_results["subnational"]["values"],
    #     iomdtm_results["subnational"]["values"],
    # )
    # extend_sources(
    #     sources,
    #     results["sources"],
    #     whowhatwhere_results["subnational"]["sources"],
    #     iomdtm_results["subnational"]["sources"],
    # )
    # update_tab("subnational", subnational)
    # extend_sources(sources, ipc_results["subnational"]["sources"])
    subnational_names = ["ipc"] + configurable_scrapers["subnational"] + ["whowhatwhere", "iomdtm"]

    runner.add_custom(who_covid)

#    runner.add_customs((who_covid, ipc, fts, food_prices, vaccination_campaigns, unhcr, inform, covax_deliveries, education_closures, education_enrolment, whowhatwhere, iomdtm))
    region = RegionAggregation(
        configuration["regional"], hrp_countries, RegionLookups.iso3_to_region_and_hrp, runner
    )
    runner.add_custom(region)
    runner.run()

    update_world(runner, outputs)
    update_regional(runner, RegionLookups.regions + ["global"], outputs)
    update_national(runner, region, hrp_countries, gho_countries, outputs)

    adminone.output_matches()
    adminone.output_ignored()
    adminone.output_errors()

    for sourceinfo in configuration["additional_sources"]:
        date = sourceinfo.get("date")
        if date is None:
            if sourceinfo.get("force_date_today", False):
                date = today.strftime("%Y-%m-%d")
        source = sourceinfo.get("source")
        source_url = sourceinfo.get("source_url")
        dataset_name = sourceinfo.get("dataset")
        if dataset_name:
            dataset = Dataset.read_from_hdx(dataset_name)
            if date is None:
                date = get_isodate_from_dataset_date(dataset, today=today)
            if source is None:
                source = dataset["dataset_source"]
            if source_url is None:
                source_url = dataset.get_hdx_url()
        sources.append((sourceinfo["indicator"], date, source, source_url))
    sources.append(get_monthly_report_source(configuration))
    sources = [list(elem) for elem in dict.fromkeys(sources)]
    update_tab("sources", sources)
    return hrp_countries
