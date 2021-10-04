import inspect
import logging
from datetime import datetime

from dateutil.relativedelta import relativedelta
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.scraper import get_date_from_dataset_date
from hdx.scraper.readers import read_tabular
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import DownloadError

logger = logging.getLogger(__name__)


def get_data(downloader, url, today, countryiso2):
    for page in range(1, 3):
        try:
            _, data = read_tabular(
                downloader,
                {
                    "url": url % (today.year, page, countryiso2),
                    "sheet": "IPC",
                    "headers": [4, 6],
                    "format": "xlsx",
                },
                fill_merged_cells=True,
            )
            data = list(data)
        except DownloadError:
            data = list()
        adm1_names = set()
        found_data = False
        for row in data:
            area = row["Area"]
            if any(
                v is not None
                for v in [
                    row["Current Phase P3+ #"],
                    row["First Projection Phase P3+ #"],
                    row["Second Projection Phase P3+ #"],
                ]
            ):
                found_data = True
            if not area or area == row["Country"]:
                continue
            adm1_name = row["Level 1 Name"]
            if adm1_name:
                adm1_names.add(adm1_name)
        if found_data is True:
            return data, adm1_names
    return None, None


def get_period(today, row, projections):
    today = today.date()
    analysis_period = ""
    for projection in projections:
        current_period = row[f"{projection} Analysis Period"]
        if current_period == "":
            continue
        start = datetime.strptime(current_period[0:8], "%b %Y").date()
        end = datetime.strptime(current_period[11:19], "%b %Y").date()
        end = end + relativedelta(day=31)
        if today < end:
            analysis_period = projection
            break
    if analysis_period == "":
        for projection in reversed(projections):
            if row[f"{projection} Analysis Period"] != "":
                analysis_period = projection
                break
    return analysis_period, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def get_ipc(configuration, today, gho_countries, adminone, downloader, scrapers=None):
    name = inspect.currentframe().f_code.co_name
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list(), list(), list()
    ipc_configuration = configuration["ipc_old"]
    url = ipc_configuration["url"]
    phases = ["3", "4", "5", "P3+"]
    projections = ["Current", "First Projection", "Second Projection"]
    national_populations = {phase: dict() for phase in phases}
    national_analysed = dict()
    national_period = dict()
    national_start = dict()
    national_end = dict()
    subnational_populations = {phase: dict() for phase in phases}
    for countryiso3 in gho_countries:
        countryiso2 = Country.get_iso2_from_iso3(countryiso3)
        data, adm1_names = get_data(downloader, url, today, countryiso2)
        if not data:
            continue
        row = data[0]
        analysis_period, start, end = get_period(today, row, projections)
        for phase in phases:
            national_populations[phase][countryiso3] = row[
                f"{analysis_period} Phase {phase} #"
            ]
        national_analysed[countryiso3] = row["Current Population Analysed #"]
        national_period[countryiso3] = analysis_period
        national_start[countryiso3] = start
        national_end[countryiso3] = end
        has_data = False
        for row in data[1:]:
            country = row["Country"]
            if adm1_names:
                if country not in adm1_names:
                    continue
                adm1_name = country
            else:
                adm1_name = row["Area"]
                if not adm1_name or adm1_name == country:
                    continue
            pcode, _ = adminone.get_pcode(countryiso3, adm1_name, "IPC")
            if not pcode:
                continue
            for phase in phases:
                population = row[f"{analysis_period} Phase {phase} #"]
                if population:
                    dict_of_lists_add(subnational_populations[phase], pcode, population)
                    has_data = True
        if not has_data:
            logger.warning(f"{countryiso3} has no values for any admin areas!")
    no_phase3plus = dict()
    for phase in phases:
        subnational_population = subnational_populations[phase]
        for pcode in subnational_population:
            populations = subnational_population[pcode]
            if len(populations) == 1:
                subnational_population[pcode] = populations[0]
            else:
                population_in_pcode = 0
                for i, population in enumerate(populations):
                    population_in_pcode += population
                subnational_population[pcode] = population_in_pcode
            if phase == "P3+":
                countryiso2 = pcode[:2]
                no_phase3plus[countryiso2] = no_phase3plus.get(countryiso2, 0) + 1
    for countryiso2, number in no_phase3plus.items():
        if number < 4:
            logger.warning(f"{Country.get_iso3_from_iso2(countryiso2)} has values for only {number} admin areas!")
    logger.info("Processed IPC")
    dataset = Dataset.read_from_hdx(ipc_configuration["dataset"])
    date = get_date_from_dataset_date(dataset, today=today)
    headers = [f"FoodInsecurityIPC{phase}" for phase in phases]
    headers.append("FoodInsecurityIPCAnalysedNum")
    headers.append("FoodInsecurityIPCAnalysisPeriod")
    headers.append("FoodInsecurityIPCAnalysisPeriodStart")
    headers.append("FoodInsecurityIPCAnalysisPeriodEnd")
    hxltags = [f"#affected+food+ipc+p{phase}+num" for phase in phases[:-1]]
    hxltags.append("#affected+food+ipc+p3plus+num")
    hxltags.append("#affected+food+ipc+analysed+num")
    hxltags.append("#date+ipc+period")
    hxltags.append("#date+ipc+start")
    hxltags.append("#date+ipc+end")
    national_outputs = [national_populations[phase] for phase in phases]
    national_outputs.append(national_analysed)
    national_outputs.append(national_period)
    national_outputs.append(national_start)
    national_outputs.append(national_end)
    subnational_outputs = [subnational_populations[phase] for phase in phases]
    return (
        [headers, hxltags],
        national_outputs,
        [headers[:-4], hxltags[:-4]],
        subnational_outputs,
        [
            (hxltag, date, dataset["dataset_source"], dataset.get_hdx_url())
            for hxltag in hxltags
        ],
    )
