import logging
from datetime import datetime

from dateutil.relativedelta import relativedelta
from hdx.location.country import Country
from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.utilities.readers import read_hdx_metadata

logger = logging.getLogger(__name__)


class IPC(BaseScraper):
    def __init__(self, datasetinfo, today, countryiso3s, adminone):
        self.phases = ["3", "4", "5"]
        self.projections = ["Current", "First Projection", "Second Projection"]
        p3plus_header = "FoodInsecurityIPCP3+"
        p3plus_hxltag = "#affected+food+ipc+p3plus+num"
        colheaders = [f"FoodInsecurityIPC{phase}" for phase in self.phases]
        colheaders.append(p3plus_header)
        colheaders.append("FoodInsecurityIPCAnalysedNum")
        colheaders.append("FoodInsecurityIPCAnalysisPeriod")
        colheaders.append("FoodInsecurityIPCAnalysisPeriodStart")
        colheaders.append("FoodInsecurityIPCAnalysisPeriodEnd")
        hxltags = [f"#affected+food+ipc+p{phase}+num" for phase in self.phases]
        hxltags.append(p3plus_hxltag)
        hxltags.append("#affected+food+ipc+analysed+num")
        hxltags.append("#date+ipc+period")
        hxltags.append("#date+ipc+start")
        hxltags.append("#date+ipc+end")
        super().__init__(
            "ipc",
            datasetinfo,
            {
                "national": (tuple(colheaders), tuple(hxltags)),
                "subnational": ((p3plus_header,), (p3plus_hxltag,)),
            },
        )
        self.today = today
        self.countryiso3s = countryiso3s
        self.adminone = adminone

    def get_period(self, projections):
        today = self.today.date()
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

    def run(self):
        base_url = self.datasetinfo["url"]
        retriever = self.get_retriever(self.name)
        countryisos = set()
        json = retriever.download_json(f"{base_url}/analyses?type=A")
        for analysis in json:
            countryiso2 = analysis["country"]
            countryiso3 = Country.get_iso3_from_iso2(countryiso2)
            if countryiso3 not in self.countryiso3s:
                continue
            countryisos.add((countryiso3, countryiso2))
        national_outputs = self.get_values("national")
        national_populations = {
            phase: national_outputs[i] for i, phase in enumerate(self.phases)
        }
        i = len(self.phases)
        national_populations["P3+"] = national_outputs[i]
        national_analysed = national_outputs[i + 1]
        national_period = national_outputs[i + 2]
        national_start = national_outputs[i + 3]
        national_end = national_outputs[i + 4]
        subnational_populations = self.get_values("subnational")[0]
        projection_names = ["Current", "First Projection", "Second Projection"]
        projection_mappings = ["", "_projected", "_second_projected"]
        analysis_dates = set()
        for countryiso3, countryiso2 in sorted(countryisos):
            url = f"{base_url}/population?country={countryiso2}"
            country_data = retriever.download_json(url)
            if country_data:
                country_data = country_data[0]
            else:
                continue
            analysis_dates.add(country_data["analysis_date"])
            projections = list()
            projections.append(country_data["current_period_dates"])
            projections.append(country_data["projected_period_dates"])
            projections.append(country_data["second_projected_period_dates"])
            projection_number, start, end = self.get_period(projections)
            sum = 0
            projection_mapping = projection_mappings[projection_number]
            for phase in self.phases:
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
                    pcode, _ = self.adminone.get_pcode(countryiso3, area["name"], "IPC")
                    if not pcode:
                        continue
                    sum = 0
                    for phase in self.phases:
                        pop = area.get(f"phase{phase}_population{projection_mapping}")
                        if pop:
                            sum += pop
                    cur_sum = subnational_populations.get(pcode)
                    if cur_sum:
                        subnational_populations[pcode] = cur_sum + sum
                    else:
                        subnational_populations[pcode] = sum
        read_hdx_metadata(self.datasetinfo)
