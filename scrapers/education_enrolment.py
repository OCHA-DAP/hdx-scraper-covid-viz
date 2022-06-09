import logging

from hdx.scraper.base_scraper import BaseScraper
from hdx.utilities.text import get_fraction_str

logger = logging.getLogger(__name__)


class EducationEnrolment(BaseScraper):
    def __init__(self, datasetinfo, closures, countryiso3s, iso3_to_region):
        super().__init__(
            "education_enrolment",
            datasetinfo,
            {
                "national": (
                    (
                        "No. pre-primary to upper-secondary learners",
                        "No. tertiary learners",
                        "No. affected learners",
                    ),
                    (
                        "#population+learners+pre_primary_to_secondary",
                        "#population+learners+tertiary",
                        "#affected+learners",
                    ),
                ),
                "regional": (
                    (
                        "No. affected learners",
                        "Percentage affected learners",
                    ),
                    (
                        "#affected+learners",
                        "#affected+learners+pct",
                    ),
                ),
            },
        )
        self.closures = closures
        self.countryiso3s = countryiso3s
        self.iso3_to_region = iso3_to_region

    def run(self) -> None:
        learners_headers, learners_iterator = self.get_reader().read(self.datasetinfo)
        learners_012, learners_3, affected_learners = self.get_values("national")
        all_learners = dict()

        for row in learners_iterator:
            countryiso = row["ISO3"]
            if not countryiso or countryiso not in self.countryiso3s:
                continue
            l_0 = row["Pre-primary (both)"]
            l_1 = row["Primary (both)"]
            l_2 = row["Secondary (both)"]
            l_3 = row["Tertiary (both)"]
            l_012 = None
            if l_0 is not None and l_0 != "-":
                l_012 = int(l_0)
            if l_1 is not None and l_1 != "-":
                l_1 = int(l_1)
                if l_012 is None:
                    l_012 = l_1
                else:
                    l_012 += l_1
            if l_2 is not None and l_2 != "-":
                l_2 = int(l_2)
                if l_012 is None:
                    l_012 = l_2
                else:
                    l_012 += l_2
            if l_012 is not None:
                learners_012[countryiso] = l_012
            if l_3 == "-":
                l_3 = None
            elif l_3 is not None:
                l_3 = int(l_3)
                learners_3[countryiso] = l_3
            no_learners = None
            if l_012 is not None:
                no_learners = l_012
                if l_3:
                    no_learners += l_3
            elif l_3 is not None:
                no_learners = l_3
            if no_learners is not None:
                all_learners[countryiso] = no_learners
                if countryiso in self.closures.fully_closed:
                    affected_learners[countryiso] = no_learners
        affected_learners_total = self.get_values("regional")[0]
        learners_total = dict()
        for countryiso in all_learners:
            country_learners = all_learners[countryiso]
            country_affected_learners = affected_learners.get(countryiso)
            for region in self.iso3_to_region[countryiso]:
                learners_total[region] = (
                    learners_total.get(region, 0) + country_learners
                )
                if country_affected_learners is not None:
                    affected_learners_total[region] = (
                        affected_learners_total.get(region, 0)
                        + country_affected_learners
                    )
        percentage_affected_learners = self.get_values("regional")[1]
        for region, no_learners in affected_learners_total.items():
            percentage_affected_learners[region] = get_fraction_str(
                no_learners, learners_total[region]
            )
