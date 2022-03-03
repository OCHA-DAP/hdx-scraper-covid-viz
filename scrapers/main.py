import logging

from hdx.data.dataset import Dataset
from hdx.location.adminone import AdminOne
from hdx.location.country import Country
from hdx.scraper.utils import get_isodate_from_dataset_date
from scrapers.utilities.extensions import extend_columns, extend_headers, extend_sources
from scrapers.utilities.region import Region
from scrapers.utilities.runner import Runner

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
from .unhcr_myanmar_idps import patch_unhcr_myanmar_idps
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
    use_live=True,
    fallbacks_root="",
):
    world = [list(), list()]
    regional = [["regionnames"], ["#region+name"]]
    national = [
        ["iso3", "countryname", "ishrp", "region"],
        ["#country+code", "#country+name", "#meta+ishrp", "#region+name"],
    ]
    subnational = [
        ["iso3", "countryname", "adm1_pcode", "adm1_name"],
        ["#country+code", "#country+name", "#adm1+code", "#adm1+name"],
    ]
    sources = [
        ("Indicator", "Date", "Source", "Url"),
        ("#indicator+name", "#date", "#meta+source", "#meta+url"),
    ]

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
    region = Region(
        configuration["regional"], today, downloader, gho_countries, hrp_countries
    )
    adminone = AdminOne(configuration)
    pcodes = adminone.pcodes
    population_lookup = dict()
    runner = Runner(
        configuration,
        gho_countries,
        adminone,
        downloader,
        basic_auths,
        today,
        population_lookup,
        fallbacks_root,
    )

    def update_tab(name, data):
        logger.info(f"Updating tab: {name}")
        for output in outputs.values():
            output.update_tab(name, data)

    results = runner.run_generic_scrapers("national", ["population"])
    national_headers = extend_headers(national, results["headers"])
    national_columns = extend_columns(
        "national",
        national,
        gho_countries,
        hrp_countries,
        region,
        None,
        national_headers,
        results["values"],
    )
    extend_sources(sources, results["sources"])
    population_lookup["GHO"] = sum(population_lookup.values())
    population_headers, population_columns = region.get_regional(
        region, national_headers, national_columns, population_lookup=population_lookup
    )
    regional_headers = extend_headers(regional, population_headers)
    extend_columns(
        "regional",
        regional,
        region.regions,
        None,
        region,
        None,
        regional_headers,
        population_columns,
    )

    results = runner.run_generic_scrapers("subnational", ["population"])
    subnational_headers = extend_headers(subnational, results["headers"])
    extend_columns(
        "subnational",
        subnational,
        pcodes,
        None,
        None,
        adminone,
        subnational_headers,
        results["values"],
    )
    who_covid = WHOCovid(
        today, outputs, hrp_countries, gho_countries, region, population_lookup
    )
    who_results = runner.run_custom_scraper(who_covid, scrapers_to_run)

    ipc = IPC(today, gho_countries, adminone, downloader)
    ipc_results = runner.run_custom_scraper(ipc, scrapers_to_run)
    if "national" in tabs:
        fts = FTS(today, outputs, gho_countries, basic_auths)
        fts_results = runner.run_custom_scraper(fts, scrapers_to_run)
        food_prices = FoodPrices(today, gho_countries, retriever, basic_auths)
        food_results = runner.run_custom_scraper(food_prices, scrapers_to_run)
        vaccination_campaigns = VaccinationCampaigns(
            today, gho_countries, downloader, outputs
        )
        campaigns_results = runner.run_custom_scraper(
            vaccination_campaigns, scrapers_to_run
        )
        unhcr = UNHCR(today, gho_countries, downloader)
        unhcr_results = runner.run_custom_scraper(unhcr, scrapers_to_run)
        inform = Inform(today, gho_countries, other_auths)
        inform_results = runner.run_custom_scraper(inform, scrapers_to_run)
        covax_deliveries = CovaxDeliveries(today, gho_countries, downloader)
        covax_results = runner.run_custom_scraper(covax_deliveries, scrapers_to_run)

        education_closures = EducationClosures(today, gho_countries, region, downloader)
        closures_results = runner.run_custom_scraper(
            education_closures, scrapers_to_run
        )
        if closures_results["national"]["values"]:
            fully_closed = education_closures.get_fully_closed(
                closures_results["national"]["values"][0]
            )
        else:
            fully_closed = tuple()

        education_enrolment = EducationEnrolment(
            fully_closed, gho_countries, region, downloader
        )
        enrolment_results = runner.run_custom_scraper(
            education_enrolment, scrapers_to_run
        )
        generic_national_results = runner.run_generic_scrapers(
            "national", scrapers_to_run
        )

        national_headers = extend_headers(
            national,
            who_results["national"]["headers"],
            generic_national_results["headers"],
            food_results["national"]["headers"],
            campaigns_results["national"]["headers"],
            fts_results["national"]["headers"],
            unhcr_results["national"]["headers"],
            inform_results["national"]["headers"],
            ipc_results["national"]["headers"],
            covax_results["national"]["headers"],
            closures_results["national"]["headers"],
            enrolment_results["national"]["headers"],
        )
        national_columns = extend_columns(
            "national",
            national,
            gho_countries,
            hrp_countries,
            region,
            None,
            national_headers,
            who_results["national"]["values"],
            generic_national_results["values"],
            food_results["national"]["values"],
            campaigns_results["national"]["values"],
            fts_results["national"]["values"],
            unhcr_results["national"]["values"],
            inform_results["national"]["values"],
            ipc_results["national"]["values"],
            covax_results["national"]["values"],
            closures_results["national"]["values"],
            enrolment_results["national"]["values"],
        )
        extend_sources(
            sources,
            who_results["national"]["sources"],
            generic_national_results["sources"],
            food_results["national"]["sources"],
            campaigns_results["national"]["sources"],
            fts_results["national"]["sources"],
            unhcr_results["national"]["sources"],
            inform_results["national"]["sources"],
            ipc_results["national"]["sources"],
            covax_results["national"]["sources"],
            closures_results["national"]["sources"],
            enrolment_results["national"]["sources"],
        )
        patch_unhcr_myanmar_idps(
            configuration, national, downloader, runner, scrapers=scrapers_to_run
        )
        update_tab("national", national)

        if "regional" in tabs:
            regional_headers, regional_columns = region.get_regional(
                region,
                national_headers,
                national_columns,
                None,
                (who_results["global"]["headers"], who_results["global"]["values"]),
                (fts_results["global"]["headers"], fts_results["global"]["values"]),
            )
            regional_headers = extend_headers(
                regional,
                regional_headers,
                closures_results["regional"]["headers"],
                enrolment_results["regional"]["headers"],
            )
            regional_columns = extend_columns(
                "regional",
                regional,
                region.regions + ["global"],
                None,
                region,
                None,
                regional_headers,
                regional_columns,
                closures_results["regional"]["values"],
                enrolment_results["regional"]["values"],
            )
            update_tab("regional", regional)
            extend_sources(
                sources,
                closures_results["regional"]["sources"],
                enrolment_results["regional"]["sources"],
            )
            if "world" in tabs:
                rgheaders, rgcolumns = region.get_world(
                    regional_headers, regional_columns
                )
                results = runner.run_generic_scrapers("global", scrapers_to_run)
                world_headers = extend_headers(
                    world,
                    who_results["gho"]["headers"],
                    fts_results["global"]["headers"],
                    results["headers"],
                    rgheaders,
                )
                extend_columns(
                    "global",
                    world,
                    None,
                    None,
                    None,
                    None,
                    world_headers,
                    who_results["gho"]["values"],
                    fts_results["global"]["values"],
                    results["values"],
                    rgcolumns,
                )
                extend_sources(
                    sources, fts_results["global"]["sources"], results["sources"]
                )
                update_tab("world", world)

    if "subnational" in tabs:
        whowhatwhere = WhoWhatWhere(today, adminone, downloader)
        whowhatwhere_results = runner.run_custom_scraper(whowhatwhere, scrapers_to_run)
        iomdtm = IOMDTM(today, adminone, downloader)
        iomdtm_results = runner.run_custom_scraper(iomdtm, scrapers_to_run)
        results = runner.run_generic_scrapers("subnational", scrapers_to_run)
        subnational_headers = extend_headers(
            subnational,
            ipc_results["subnational"]["headers"],
            results["headers"],
            whowhatwhere_results["subnational"]["headers"],
            iomdtm_results["subnational"]["headers"],
        )
        extend_columns(
            "subnational",
            subnational,
            pcodes,
            None,
            None,
            adminone,
            subnational_headers,
            ipc_results["subnational"]["values"],
            results["values"],
            whowhatwhere_results["subnational"]["values"],
            iomdtm_results["subnational"]["values"],
        )
        extend_sources(
            sources,
            results["sources"],
            whowhatwhere_results["subnational"]["sources"],
            iomdtm_results["subnational"]["sources"],
        )
        update_tab("subnational", subnational)
    extend_sources(sources, ipc_results["subnational"]["sources"])

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

    fallbacks_used = runner.get_fallbacks_used()
    if fallbacks_used:
        logger.error(f"Fallbacks were used: {', '.join(fallbacks_used)}")
        fail = True
    else:
        fail = False
    return hrp_countries, fail
