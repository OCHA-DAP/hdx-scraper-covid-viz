import logging

from hdx.data.dataset import Dataset
from hdx.location.adminone import AdminOne
from hdx.location.country import Country
from hdx.scraper.utils import get_isodate_from_dataset_date
from scrapers.utilities.extensions import extend_columns, extend_headers, extend_sources
from scrapers.utilities.fallbacks import Fallbacks
from scrapers.utilities.region import Region

from .covax_deliveries import get_covax_deliveries
from .education_closures import EducationClosures
from .education_enrolment import get_education_enrolment
from .food_prices import add_food_prices
from .fts import get_fts
from .inform import get_inform
from .iom_dtm import get_iom_dtm
from .ipc_old import get_ipc
from .monthly_report import get_monthly_report_source
from .unhcr import get_unhcr
from .unhcr_myanmar_idps import patch_unhcr_myanmar_idps
from .vaccination_campaigns import add_vaccination_campaigns
from .who_covid import get_who_covid
from .whowhatwhere import get_whowhatwhere

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

    today_str = today.strftime("%Y-%m-%d")
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
    fallbacks = Fallbacks(
        configuration,
        gho_countries,
        adminone,
        downloader,
        basic_auths,
        today,
        today_str,
        population_lookup,
        fallbacks_root,
    )

    def update_tab(name, data):
        logger.info(f"Updating tab: {name}")
        for output in outputs.values():
            output.update_tab(name, data)

    results = fallbacks.run_generic_scrapers("national", ["population"])
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

    results = fallbacks.run_generic_scrapers("subnational", ["population"])
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
    (
        covid_wheaders,
        covid_wcolumns,
        covid_ghocolumns,
        covid_headers,
        covid_columns,
        covid_sources,
    ) = get_who_covid(
        configuration,
        today,
        outputs,
        hrp_countries,
        gho_countries,
        region,
        population_lookup,
        scrapers_to_run,
    )
    extend_sources(sources, covid_sources)

    # ipc_headers, ipc_columns, ipc_sheaders, ipc_scolumns, ipc_sources = get_ipc(
    #     configuration, today, gho_countries, adminone, other_auths, scrapers
    # )
    ipc_headers, ipc_columns, ipc_sheaders, ipc_scolumns, ipc_sources = get_ipc(
        configuration, today, gho_countries, adminone, downloader, scrapers_to_run
    )
    if "national" in tabs:
        (
            fts_wheaders,
            fts_wcolumns,
            fts_wsources,
            fts_headers,
            fts_columns,
            fts_sources,
        ) = get_fts(
            configuration, today, today_str, gho_countries, basic_auths, scrapers_to_run
        )
        food_headers, food_columns, food_sources = add_food_prices(
            configuration, today, gho_countries, retriever, basic_auths, scrapers_to_run
        )
        (
            campaign_headers,
            campaign_columns,
            campaign_sources,
        ) = add_vaccination_campaigns(
            configuration, today, gho_countries, downloader, outputs, scrapers_to_run
        )
        unhcr_headers, unhcr_columns, unhcr_sources = get_unhcr(
            configuration, today, today_str, gho_countries, downloader, scrapers_to_run
        )
        inform_headers, inform_columns, inform_sources = get_inform(
            configuration, today, gho_countries, other_auths, scrapers_to_run
        )


        covax_headers, covax_columns, covax_sources = get_covax_deliveries(
            configuration, today, gho_countries, downloader, scrapers_to_run
        )

        education_closures = EducationClosures(today, gho_countries, region, downloader)
        closures_results = Fallbacks.with_fallbacks(
            configuration, education_closures, scrapers_to_run
        )
        if closures_results:
            fully_closed = education_closures.get_fully_closed(
                closures_results["national"]["values"][0]
            )
            closures_rheaders = closures_results["regional"]["headers"]
            closures_rcolumns = closures_results["regional"]["values"]
            closures_rsources = closures_results["regional"]["sources"]
            closures_headers = closures_results["national"]["headers"]
            closures_columns = closures_results["national"]["values"]
            closures_sources = closures_results["national"]["sources"]
        else:
            fully_closed = tuple()
            closures_rheaders = tuple()
            closures_rcolumns = tuple()
            closures_rsources = tuple()
            closures_headers = tuple()
            closures_columns = tuple()
            closures_sources = tuple()

        (
            enrolment_rheaders,
            enrolment_rcolumns,
            enrolment_rsources,
            enrolment_headers,
            enrolment_columns,
            enrolment_sources,
        ) = get_education_enrolment(
            configuration,
            fully_closed,
            gho_countries,
            region,
            downloader,
            scrapers_to_run,
        )
        results = fallbacks.run_generic_scrapers("national", scrapers_to_run)
        national_headers = extend_headers(
            national,
            covid_headers,
            results["headers"],
            food_headers,
            campaign_headers,
            fts_headers,
            unhcr_headers,
            inform_headers,
            ipc_headers,
            covax_headers,
            closures_headers,
            enrolment_headers,
        )
        national_columns = extend_columns(
            "national",
            national,
            gho_countries,
            hrp_countries,
            region,
            None,
            national_headers,
            covid_columns,
            results["values"],
            food_columns,
            campaign_columns,
            fts_columns,
            unhcr_columns,
            inform_columns,
            ipc_columns,
            covax_columns,
            closures_columns,
            enrolment_columns,
        )
        extend_sources(
            sources,
            results["sources"],
            food_sources,
            campaign_sources,
            fts_sources,
            unhcr_sources,
            inform_sources,
            covax_sources,
            closures_sources,
            enrolment_sources,
        )
        patch_unhcr_myanmar_idps(
            configuration, national, downloader, scrapers=scrapers_to_run
        )
        update_tab("national", national)

        if "regional" in tabs:
            regional_headers, regional_columns = region.get_regional(
                region,
                national_headers,
                national_columns,
                None,
                (covid_wheaders, covid_wcolumns),
                (fts_wheaders, fts_wcolumns),
            )
            regional_headers = extend_headers(
                regional, regional_headers, closures_rheaders, enrolment_rheaders
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
                closures_rcolumns,
                enrolment_rcolumns,
            )
            update_tab("regional", regional)
            extend_sources(sources, closures_rsources, enrolment_rsources)
            if "world" in tabs:
                rgheaders, rgcolumns = region.get_world(
                    regional_headers, regional_columns
                )
                results = fallbacks.run_generic_scrapers("global", scrapers_to_run)
                world_headers = extend_headers(
                    world, covid_wheaders, fts_wheaders, results["headers"], rgheaders
                )
                extend_columns(
                    "global",
                    world,
                    None,
                    None,
                    None,
                    None,
                    world_headers,
                    covid_ghocolumns,
                    fts_wcolumns,
                    results["values"],
                    rgcolumns,
                )
                extend_sources(sources, fts_wsources, results["sources"])
                update_tab("world", world)

    if "subnational" in tabs:
        (
            whowhatwhere_headers,
            whowhatwhere_columns,
            whowhatwhere_sources,
        ) = get_whowhatwhere(
            configuration, today_str, adminone, downloader, scrapers_to_run
        )
        iomdtm_headers, iomdtm_columns, iomdtm_sources = get_iom_dtm(
            configuration, today_str, adminone, downloader, scrapers_to_run
        )
        results = fallbacks.run_generic_scrapers("subnational", scrapers_to_run)
        subnational_headers = extend_headers(
            subnational,
            ipc_sheaders,
            results["headers"],
            whowhatwhere_headers,
            iomdtm_headers,
        )
        extend_columns(
            "subnational",
            subnational,
            pcodes,
            None,
            None,
            adminone,
            subnational_headers,
            ipc_scolumns,
            results["values"],
            whowhatwhere_columns,
            iomdtm_columns,
        )
        extend_sources(
            sources, results["sources"], whowhatwhere_sources, iomdtm_sources
        )
        update_tab("subnational", subnational)
    extend_sources(sources, ipc_sources)

    adminone.output_matches()
    adminone.output_ignored()
    adminone.output_errors()

    for sourceinfo in configuration["additional_sources"]:
        date = sourceinfo.get("date")
        if date is None:
            if sourceinfo.get("force_date_today", False):
                date = today_str
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

    fallbacks_used = fallbacks.get_fallbacks_used()
    if fallbacks_used:
        logger.error(f"Fallbacks were used: {', '.join(fallbacks_used)}")
        fail = True
    else:
        fail = False
    return hrp_countries, fail
