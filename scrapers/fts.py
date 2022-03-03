import logging
import re
from typing import Dict

from dateutil.relativedelta import relativedelta
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import Download
from hdx.utilities.text import earliest_index, get_fraction_str, multiple_replace
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class FTSException(Exception):
    pass


class FTS(BaseScraper):
    name = "fts"
    base_hxltags = [
        "#value+funding+hrp+required+usd",
        "#value+funding+hrp+total+usd",
        "#value+funding+hrp+pct",
    ]
    national_hxltags = base_hxltags + [
        "#value+covid+funding+hrp+total+usd",
        "#value+funding+other+plan_name",
        "#value+funding+other+required+usd",
        "#value+funding+other+total+usd",
        "#value+funding+other+pct",
    ]
    reg_reqfund_hxltags = {
        "Plan Name": "#value+funding+regional+plan_name",
        "Requirements": "#value+funding+regional+required+usd",
        "Funding": "#value+funding+regional+total+usd",
        "PercentFunded": "#value+funding+regional+pct",
    }

    headers = {
        "national": (
            (
                "RequiredHRPFunding",
                "HRPFunding",
                "HRPPercentFunded",
                "HRPCovidFunding",
                "OtherPlans",
                "RequiredOtherPlansFunding",
                "OtherPlansFunding",
                "OtherPlansPercentFunded",
            ),
            tuple(national_hxltags),
        ),
        "global": (
            ("RequiredFunding", "Funding", "PercentFunded"),
            tuple(base_hxltags),
        ),
    }

    def __init__(self, today, outputs, countryiso3s, basic_auths):
        super().__init__()
        self.today = today
        self.outputs = outputs
        self.countryiso3s = countryiso3s
        self.basic_auths = basic_auths

    def download(self, url, downloader):
        r = downloader.download(url)
        json = r.json()
        status = json["status"]
        if status != "ok":
            raise FTSException(f"{url} gives status {status}")
        return json

    def download_data(self, url, downloader):
        return self.download(url, downloader)["data"]

    def get_covid_funding(self, plan_id, plan_name, fundingobjects):
        if len(fundingobjects) != 0:
            objectsbreakdown = fundingobjects[0].get("objectsBreakdown")
            if objectsbreakdown:
                for fundobj in objectsbreakdown:
                    fund_id = fundobj.get("id")
                    fund = fundobj["totalFunding"]
                    if fund_id and fund_id == plan_id:
                        logger.info(f"{plan_name}: Funding={fund}")
                        return fund
        return None

    def get_requirements_and_funding_location(
        self, base_url, plan, countryid_iso3mapping, downloader
    ):
        countryreqs, countryfunds = dict(), dict()
        plan_id = plan["id"]
        url = f"{base_url}1/fts/flow/custom-search?planid={plan_id}&groupby=location"
        data = self.download_data(url, downloader)
        requirements = data["requirements"]
        totalreq = requirements["totalRevisedReqs"]
        countryreq_is_totalreq = True
        for reqobj in requirements["objects"]:
            countryid = reqobj.get("id")
            if not countryid:
                continue
            countryiso = countryid_iso3mapping.get(str(countryid))
            if not countryiso:
                continue
            if countryiso not in self.countryiso3s:
                continue
            req = reqobj.get("revisedRequirements")
            if req:
                countryreqs[countryiso] = req
                if req != totalreq:
                    countryreq_is_totalreq = False
        if countryreq_is_totalreq:
            countryreqs = dict()
            logger.info(
                f"{plan_id} has same country requirements as total requirements!"
            )

        fundingobjects = data["report3"]["fundingTotals"]["objects"]
        if len(fundingobjects) != 0:
            objectsbreakdown = fundingobjects[0].get("objectsBreakdown")
            if objectsbreakdown:
                for fundobj in objectsbreakdown:
                    countryid = fundobj.get("id")
                    if not countryid:
                        continue
                    countryiso = countryid_iso3mapping.get(countryid)
                    if not countryiso:
                        continue
                    if countryiso not in self.countryiso3s:
                        continue
                    countryfunds[countryiso] = fundobj["totalFunding"]
        return countryreqs, countryfunds

    @staticmethod
    def map_planname(origname):
        name = None
        origname_simplified = origname.replace("  ", " ")
        origname_simplified = re.sub(
            r"\d\d\d\d(-\d\d\d\d)?", "", origname_simplified
        )  # strip date
        origname_simplified = re.sub(
            r"[\(\[].*?[\)\]]", "", origname_simplified
        )  # strip stuff in brackets
        origname_simplified = origname_simplified.strip()
        origname_lower = origname_simplified.lower()
        regional_strings = ["regional", "refugee", "migrant"]
        if any(x in origname_lower for x in regional_strings):
            location = None
            try:
                for_index = origname_lower.index(" for ")
                location = origname_simplified[for_index + 5 :]
                location = location.replace("the", "").strip()
            except ValueError:
                non_location_index = earliest_index(origname_lower, regional_strings)
                if non_location_index:
                    location = origname_simplified[: non_location_index - 1]
            if location:
                name = f"{location} Regional"
        if not name:
            name = multiple_replace(
                origname_simplified,
                {
                    "Plan": "",
                    "Intersectoral": "",
                    "Joint": "",
                    "Flash Appeal": "Appeal",
                    "Emergency Response": "Emergency",
                },
            )
            name = name.strip()
        if origname == name:
            logger.info(f'Plan name "{name}" not simplified')
        else:
            logger.info(f'Plan name "{name}" simplified from "{origname}"')
        return name

    def run(self, datasetinfo: Dict) -> None:
        (
            hrp_requirements,
            hrp_funding,
            hrp_percentage,
            hrp_covid_funding,
            other_planname,
            other_requirements,
            other_funding,
            other_percentage,
        ) = self.get_values("national")

        def add_other_requirements_and_funding(iso3, name, req, fund, pct):
            dict_of_lists_add(other_planname, iso3, name)
            if req:
                dict_of_lists_add(other_requirements, iso3, req)
                if fund:
                    dict_of_lists_add(other_percentage, iso3, pct)
                else:
                    dict_of_lists_add(other_percentage, iso3, None)
            else:
                dict_of_lists_add(other_requirements, iso3, None)
                dict_of_lists_add(other_percentage, iso3, None)
            if fund:
                dict_of_lists_add(other_funding, iso3, fund)
            else:
                dict_of_lists_add(other_funding, iso3, None)

        base_url = datasetinfo["url"]

        with Download(
            basic_auth=self.basic_auths.get("fts"), rate_limit={"calls": 1, "period": 1}
        ) as downloader:
            curdate = self.today - relativedelta(months=1)
            url = f"{base_url}2/fts/flow/plan/overview/progress/{curdate.year}"
            data = self.download_data(url, downloader)
            plans = data["plans"]
            plan_ids = ",".join([str(plan["id"]) for plan in plans])
            url = f"{base_url}1/fts/flow/custom-search?emergencyid=911&planid={plan_ids}&groupby=plan"
            funding_data = self.download_data(url, downloader)
            fundingtotals = funding_data["report3"]["fundingTotals"]
            fundingobjects = fundingtotals["objects"]
            reg_reqfund_output = [
                list(self.reg_reqfund_hxltags.keys()),
                list(self.reg_reqfund_hxltags.values()),
            ]
            for plan in plans:
                plan_id = str(plan["id"])
                plan_name = plan["name"]
                allreq = plan["requirements"]["revisedRequirements"]
                funding = plan.get("funding")
                if funding:
                    allfund = funding["totalFunding"]
                    allpct = get_fraction_str(funding["progress"], 100)
                else:
                    allfund = None
                    allpct = None
                if plan.get("customLocationCode") == "COVD":
                    continue

                countries = plan["countries"]
                countryid_iso3mapping = dict()
                for country in countries:
                    countryiso = country["iso3"]
                    if countryiso:
                        countryid = country["id"]
                        countryid_iso3mapping[str(countryid)] = countryiso
                if len(countryid_iso3mapping) == 0:
                    continue
                if len(countryid_iso3mapping) == 1:
                    countryiso = countryid_iso3mapping.popitem()[1]
                    if not countryiso or countryiso not in self.countryiso3s:
                        continue
                    plan_type = plan["planType"]["name"].lower()
                    if plan_type == "humanitarian response plan":
                        if allreq:
                            hrp_requirements[countryiso] = allreq
                        else:
                            hrp_requirements[countryiso] = None
                        if allfund and allreq:
                            hrp_funding[countryiso] = allfund
                            hrp_percentage[countryiso] = allpct
                        covidfund = self.get_covid_funding(
                            plan_id, plan_name, fundingobjects
                        )
                        if covidfund is not None:
                            hrp_covid_funding[countryiso] = covidfund
                    else:
                        plan_name = self.map_planname(plan_name)
                        add_other_requirements_and_funding(
                            countryiso, plan_name, allreq, allfund, allpct
                        )
                        if plan_type == "regional response plan":
                            reg_reqfund_output.append(
                                [plan_name, allreq, allfund, allpct]
                            )
                else:
                    (
                        countryreqs,
                        countryfunds,
                    ) = self.get_requirements_and_funding_location(
                        base_url, plan, countryid_iso3mapping, downloader
                    )
                    plan_name = self.map_planname(plan_name)
                    reg_reqfund_output.append([plan_name, allreq, allfund, allpct])
                    for countryiso in countryfunds:
                        countryfund = countryfunds[countryiso]
                        countryreq = countryreqs.get(countryiso)
                        if countryreq:
                            countrypct = get_fraction_str(countryfund, countryreq)
                        else:
                            countrypct = None
                        add_other_requirements_and_funding(
                            countryiso, plan_name, countryreq, countryfund, countrypct
                        )
                    for countryiso in countryreqs:
                        if countryiso in countryfunds:
                            continue
                        add_other_requirements_and_funding(
                            countryiso, plan_name, countryreqs[countryiso], None, None
                        )

            def create_output(vallist):
                strings = list()
                for val in vallist:
                    if val is None:
                        strings.append("")
                    else:
                        strings.append(str(val))
                return "|".join(strings)

            for countryiso in other_planname:
                other_planname[countryiso] = create_output(other_planname[countryiso])
                other_requirements[countryiso] = create_output(
                    other_requirements[countryiso]
                )
                other_funding[countryiso] = create_output(other_funding[countryiso])
                other_percentage[countryiso] = create_output(
                    other_percentage[countryiso]
                )
            total_allreq = data["totals"]["revisedRequirements"]
            total_allfund = data["totals"]["totalFunding"]
            total_allpercent = get_fraction_str(data["totals"]["progress"], 100)
            global_values = self.get_values("global")
            global_values[0]["global"] = total_allreq
            global_values[1]["global"] = total_allfund
            global_values[2]["global"] = total_allpercent
            tabname = "regional_reqfund"
            self.outputs["gsheets"].update_tab(tabname, reg_reqfund_output)
            self.outputs["excel"].update_tab(tabname, reg_reqfund_output)
            self.outputs["json"].update_tab(tabname, reg_reqfund_output)
            datasetinfo["date"] = self.today
            logger.info("Processed FTS")
