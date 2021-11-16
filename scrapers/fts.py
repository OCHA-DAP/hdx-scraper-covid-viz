import inspect
import logging
import re

from dateutil.relativedelta import relativedelta
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import Download
from hdx.utilities.text import get_fraction_str, multiple_replace, earliest_index

logger = logging.getLogger(__name__)


class FTSException(Exception):
    pass


def download(url, downloader):
    r = downloader.download(url)
    json = r.json()
    status = json["status"]
    if status != "ok":
        raise FTSException(f"{url} gives status {status}")
    return json


def download_data(url, downloader):
    return download(url, downloader)["data"]


def get_covid_funding(plan_id, plan_name, fundingobjects):
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
    base_url, plan, countryid_iso3mapping, countryiso3s, downloader
):
    allreqs, allfunds = dict(), dict()
    plan_id = plan["id"]
    url = f"{base_url}1/fts/flow/custom-search?planid={plan_id}&groupby=location"
    data = download_data(url, downloader)
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
        if countryiso not in countryiso3s:
            continue
        req = reqobj.get("revisedRequirements")
        if req:
            allreqs[countryiso] = req
            if req != totalreq:
                countryreq_is_totalreq = False
    if countryreq_is_totalreq:
        allreqs = dict()
        logger.info(f"{plan_id} has same country requirements as total requirements!")

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
                if countryiso not in countryiso3s:
                    continue
                allfunds[countryiso] = fundobj["totalFunding"]
    return allreqs, allfunds


def map_planname(origname):
    name = None
    origname_simplified = origname.replace("  ", " ")
    origname_simplified = re.sub(r"\d\d\d\d", "", origname_simplified)  # strip date
    origname_simplified = re.sub(r"[\(\[].*?[\)\]]", "", origname_simplified)  # strip stuff in brackets
    origname_simplified = origname_simplified.strip()
    origname_lower = origname_simplified.lower()
    if "refugee" or "migrant" in origname_lower:
        location = None
        try:
            for_index = origname_lower.index(" for ")
            location = origname_simplified[for_index+5:]
            location = location.replace("the", "").strip()
        except ValueError:
            non_location_index = earliest_index(origname_lower, ["regional", "refugee"])
            if non_location_index:
                location = origname_simplified[:non_location_index-1]
        if location:
            name = f"{location} Regional"
    if not name:
        name = multiple_replace(origname_simplified, {"Plan": "", "Intersectoral": "", "Joint": ""})
        name = name.strip()
    if origname == name:
        logger.info(f'Plan name "{name}" not simplified')
    else:
        logger.info(f'Plan name "{name}" simplified from "{origname}"')
    return name


def get_fts(configuration, today, today_str, countryiso3s, basic_auths, scrapers=None):
    name = inspect.currentframe().f_code.co_name
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list(), list(), list(), list()
    hrp_requirements = dict()
    hrp_funding = dict()
    hrp_percentage = dict()
    hrp_covid_funding = dict()
    other_planname = dict()
    other_requirements = dict()
    other_funding = dict()
    other_percentage = dict()

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

    fts_configuration = configuration["fts"]
    base_url = fts_configuration["url"]

    with Download(
        basic_auth=basic_auths.get("fts"), rate_limit={"calls": 1, "period": 1}
    ) as downloader:
        curdate = today - relativedelta(months=1)
        url = f"{base_url}2/fts/flow/plan/overview/progress/{curdate.year}"
        data = download_data(url, downloader)
        plans = data["plans"]
        plan_ids = ",".join([str(plan["id"]) for plan in plans])
        url = f"{base_url}1/fts/flow/custom-search?emergencyid=911&planid={plan_ids}&groupby=plan"
        funding_data = download_data(url, downloader)
        fundingtotals = funding_data["report3"]["fundingTotals"]
        fundingobjects = fundingtotals["objects"]
        for plan in plans:
            plan_id = str(plan["id"])
            plan_name = plan["name"]
            allreq = plan["requirements"]["revisedRequirements"]
            funding = plan.get("funding")
            if funding:
                allfund = funding["totalFunding"]
            else:
                allfund = None
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
                if not countryiso or countryiso not in countryiso3s:
                    continue
                plan_type = plan["planType"]["name"].lower()
                if funding:
                    allpct = get_fraction_str(funding["progress"], 100)
                else:
                    allpct = None
                if plan_type == "humanitarian response plan":
                    if allreq:
                        hrp_requirements[countryiso] = allreq
                    else:
                        hrp_requirements[countryiso] = None
                    if allfund and allreq:
                        hrp_funding[countryiso] = allfund
                        hrp_percentage[countryiso] = allpct
                    covidfund = get_covid_funding(plan_id, plan_name, fundingobjects)
                    if covidfund is not None:
                        hrp_covid_funding[countryiso] = covidfund
                else:
                    plan_name = map_planname(plan_name)
                    add_other_requirements_and_funding(
                        countryiso, plan_name, allreq, allfund, allpct
                    )
            else:
                allreqs, allfunds = get_requirements_and_funding_location(
                    base_url, plan, countryid_iso3mapping, countryiso3s, downloader
                )
                plan_name = map_planname(plan_name)
                for countryiso in allfunds:
                    allfund = allfunds[countryiso]
                    allreq = allreqs.get(countryiso)
                    if allreq:
                        allpct = get_fraction_str(allfund, allreq)
                    else:
                        allpct = None
                    add_other_requirements_and_funding(
                        countryiso, plan_name, allreq, allfund, allpct
                    )
                for countryiso in allreqs:
                    if countryiso in allfunds:
                        continue
                    add_other_requirements_and_funding(
                        countryiso, plan_name, allreqs[countryiso], None, None
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
            other_percentage[countryiso] = create_output(other_percentage[countryiso])
        total_allreq = data["totals"]["revisedRequirements"]
        total_allfund = data["totals"]["totalFunding"]
        total_allpercent = get_fraction_str(data["totals"]["progress"], 100)
        logger.info("Processed FTS")
        ghxltags = [
            "#value+funding+hrp+required+usd",
            "#value+funding+hrp+total+usd",
            "#value+funding+hrp+pct",
        ]
        hxltags = ghxltags + [
            "#value+covid+funding+hrp+total+usd",
            "#value+funding+other+plan_name",
            "#value+funding+other+required+usd",
            "#value+funding+other+total+usd",
            "#value+funding+other+pct",
        ]
        total_allreq = {"global": total_allreq}
        total_allfund = {"global": total_allfund}
        total_allpercent = {"global": total_allpercent}
        return (
            [["RequiredFunding", "Funding", "PercentFunded"], ghxltags],
            [total_allreq, total_allfund, total_allpercent],
            [
                (hxltag, today_str, "OCHA", fts_configuration["source_url"])
                for hxltag in ghxltags
            ],
            [
                [
                    "RequiredHRPFunding",
                    "HRPFunding",
                    "HRPPercentFunded",
                    "HRPCovidFunding",
                    "OtherPlans",
                    "RequiredOtherPlansFunding",
                    "OtherPlansFunding",
                    "OtherPlansPercentFunded",
                ],
                hxltags,
            ],
            [
                hrp_requirements,
                hrp_funding,
                hrp_percentage,
                hrp_covid_funding,
                other_planname,
                other_requirements,
                other_funding,
                other_percentage,
            ],
            [
                (hxltag, today_str, "OCHA", fts_configuration["source_url"])
                for hxltag in hxltags
            ],
        )
