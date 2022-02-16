import inspect
import logging
from datetime import datetime

from dateutil.relativedelta import relativedelta
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.scraper.utilities import get_isodate_from_dataset_date
from hdx.utilities.downloader import Download

logger = logging.getLogger(__name__)


def get_period(today, projections):
    today = today.date()
    projection_number = None
    for i, projection in enumerate(projections):
        if projection == "":
            continue
        start = datetime.strptime(projection[0:8], "%b %Y").date()
        end = datetime.strptime(projection[11:19], "%b %Y").date() + relativedelta(
            day=31
        )
        if today < end:
            projection_number = i
            break
    if projection_number is None:
        for i, projection in reversed(list(enumerate(projections))):
            if projection != "":
                projection_number = i
                break
    return projection_number, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def get_ipc(configuration, today, gho_countries, adminone, other_auths, scrapers=None):
    name = inspect.currentframe().f_code.co_name
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list(), list(), list()
    ipc_configuration = configuration["ipc"]
    base_url = ipc_configuration["url"]
    with Download(
        rate_limit={"calls": 1, "period": 0.1},
        extra_params_dict={"key": other_auths["ipc"]},
    ) as downloader:
        countryisos = set()
        downloader.download(f"{base_url}/analyses?type=A")
        for analysis in downloader.get_json():
            countryiso2 = analysis["country"]
            countryiso3 = Country.get_iso3_from_iso2(countryiso2)
            if countryiso3 not in gho_countries:
                continue
            countryisos.add((countryiso3, countryiso2))
        phases = ["3", "4", "5"]
        national_populations = {phase: dict() for phase in phases}
        national_populations["P3+"] = dict()
        national_analysed = dict()
        national_period = dict()
        national_start = dict()
        national_end = dict()
        subnational_populations = dict()
        projection_names = ["Current", "First Projection", "Second Projection"]
        projection_mappings = ["", "_projected", "_second_projected"]
        analysis_dates = set()
        for countryiso3, countryiso2 in sorted(countryisos):
            downloader.download(f"{base_url}/population?country={countryiso2}")
            country_data = downloader.get_json()
            if country_data:
                country_data = country_data[-1]
            else:
                continue
            analysis_dates.add(country_data["analysis_date"])
            projections = list()
            projections.append(country_data["current_period_dates"])
            projections.append(country_data["projected_period_dates"])
            projections.append(country_data["second_projected_period_dates"])
            projection_number, start, end = get_period(today, projections)
            sum = 0
            projection_mapping = projection_mappings[projection_number]
            for phase in phases:
                population = country_data[
                    f"phase{phase}_population{projection_mapping}"
                ]
                national_populations[phase][countryiso3] = population
                sum += population
            national_populations["P3+"][countryiso3] = sum
            national_analysed[countryiso3] = country_data[
                f"estimated_population{projection_mapping}"
            ]
            national_period[countryiso3] = projection_names[projection_number]
            national_start[countryiso3] = start
            national_end[countryiso3] = end
            areas = country_data.get("areas", country_data.get("groups"))
            if areas:
                for area in areas:
                    pcode, _ = adminone.get_pcode(countryiso3, area["name"], "IPC")
                    if not pcode:
                        continue
                    sum = 0
                    for phase in phases:
                        pop = area.get(f"phase{phase}_population{projection_mapping}")
                        if pop:
                            sum += pop
                    cur_sum = subnational_populations.get(pcode)
                    if cur_sum:
                        subnational_populations[pcode] = cur_sum + sum
                    else:
                        subnational_populations[pcode] = sum
    dataset = Dataset.read_from_hdx(ipc_configuration["dataset"])
    date = get_isodate_from_dataset_date(dataset, today=today)
    #    analysis_dates = [(datetime.strptime(date, "%b %Y").date() + relativedelta(day=31)) for date in analysis_dates]
    #    date = sorted(analysis_dates)[-1]
    headers = [f"FoodInsecurityIPC{phase}" for phase in phases]
    headers.append("FoodInsecurityIPCP3+")
    headers.append("FoodInsecurityIPCAnalysedNum")
    headers.append("FoodInsecurityIPCAnalysisPeriod")
    headers.append("FoodInsecurityIPCAnalysisPeriodStart")
    headers.append("FoodInsecurityIPCAnalysisPeriodEnd")
    hxltags = [f"#affected+food+ipc+p{phase}+num" for phase in phases]
    hxltags.append("#affected+food+ipc+p3plus+num")
    hxltags.append("#affected+food+ipc+analysed+num")
    hxltags.append("#date+ipc+period")
    hxltags.append("#date+ipc+start")
    hxltags.append("#date+ipc+end")
    national_outputs = [national_populations[phase] for phase in phases]
    national_outputs.append(national_populations["P3+"])
    national_outputs.append(national_analysed)
    national_outputs.append(national_period)
    national_outputs.append(national_start)
    national_outputs.append(national_end)
    subnational_outputs = [subnational_populations]
    logger.info("Processed IPC")
    return (
        [headers, hxltags],
        national_outputs,
        [["FoodInsecurityIPCP3+"], ["#affected+food+ipc+p3plus+num"]],
        subnational_outputs,
        [
            (hxltag, date, dataset["dataset_source"], dataset.get_hdx_url())
            for hxltag in hxltags
        ],
    )
