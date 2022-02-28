import logging
from datetime import datetime

from dateutil.relativedelta import relativedelta
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.scraper.readers import read_hdx_metadata, read_tabular
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import DownloadError
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class IPC(BaseScraper):
    name = "ipc"
    phases = ["3", "4", "5", "P3+"]
    projections = ["Current", "First Projection", "Second Projection"]
    colheaders = [f"FoodInsecurityIPC{phase}" for phase in phases]
    colheaders.append("FoodInsecurityIPCAnalysedNum")
    colheaders.append("FoodInsecurityIPCAnalysisPeriod")
    colheaders.append("FoodInsecurityIPCAnalysisPeriodStart")
    colheaders.append("FoodInsecurityIPCAnalysisPeriodEnd")
    hxltags = [f"#affected+food+ipc+p{phase}+num" for phase in phases[:-1]]
    hxltags.append("#affected+food+ipc+p3plus+num")
    hxltags.append("#affected+food+ipc+analysed+num")
    hxltags.append("#date+ipc+period")
    hxltags.append("#date+ipc+start")
    hxltags.append("#date+ipc+end")

    headers = {
        "national": (tuple(colheaders), tuple(hxltags)),
        "subnational": (tuple(colheaders[:-4]), tuple(hxltags[:-4])),
    }

    def __init__(self, today, gho_countries, adminone, downloader):
        super().__init__()
        self.today = today
        self.gho_countries = gho_countries
        self.adminone = adminone
        self.downloader = downloader

    def get_data(self, url, countryiso2):
        for page in range(1, 3):
            try:
                _, data = read_tabular(
                    self.downloader,
                    {
                        "url": url % (self.today.year, page, countryiso2),
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
                    for v in (
                        row["Current Phase P3+ #"],
                        row["First Projection Phase P3+ #"],
                        row["Second Projection Phase P3+ #"],
                    )
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

    def get_period(self, row, projections):
        start = None
        end = None
        today = self.today.date()
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

    def run(self, datasetinfo):
        url = datasetinfo["url"]
        national_populations = {phase: dict() for phase in self.phases}
        national_analysed = dict()
        national_period = dict()
        national_start = dict()
        national_end = dict()
        subnational_populations = {phase: dict() for phase in self.phases}
        failure_counter = 0
        for countryiso3 in self.gho_countries:
            countryiso2 = Country.get_iso2_from_iso3(countryiso3)
            data, adm1_names = self.get_data(url, countryiso2)
            if not data:
                failure_counter += 1
                if failure_counter == 7:
                    raise HDXError("IPC is broken!")
                continue
            else:
                failure_counter = 0
            row = data[0]
            analysis_period, start, end = self.get_period(row, self.projections)
            for phase in self.phases:
                national_populations[phase][countryiso3] = row[
                    f"{analysis_period} Phase {phase} #"
                ]
            national_analysed[countryiso3] = row["Current Population Analysed #"]
            national_period[countryiso3] = analysis_period
            national_start[countryiso3] = start
            national_end[countryiso3] = end
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
                pcode, _ = self.adminone.get_pcode(countryiso3, adm1_name, "IPC")
                if not pcode:
                    continue
                for phase in self.phases:
                    population = row[f"{analysis_period} Phase {phase} #"]
                    if population:
                        dict_of_lists_add(
                            subnational_populations[phase], pcode, population
                        )
        for phase in self.phases:
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
        read_hdx_metadata(datasetinfo)
        national_outputs = [national_populations[phase] for phase in self.phases]
        national_outputs.append(national_analysed)
        national_outputs.append(national_period)
        national_outputs.append(national_start)
        national_outputs.append(national_end)
        self.values["national"] = tuple(national_outputs)
        self.values["subnational"] = tuple(
            subnational_populations[phase] for phase in self.phases
        )
        logger.info("Processed IPC")
