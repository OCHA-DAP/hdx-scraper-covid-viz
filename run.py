import argparse
import logging
import sys
from datetime import datetime
from os import getenv
from os.path import join

from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.scraper.exceloutput import ExcelOutput
from hdx.scraper.googlesheets import GoogleSheets
from hdx.scraper.jsonoutput import JsonOutput
from hdx.scraper.nooutput import NoOutput
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from scrapers.main import get_indicators

setup_logging()
logger = logging.getLogger()


VERSION = 4.0


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ua", "--user_agent", default=None, help="user agent")
    parser.add_argument("-pp", "--preprefix", default=None, help="preprefix")
    parser.add_argument("-hs", "--hdx_site", default=None, help="HDX site to use")
    parser.add_argument(
        "-xl", "--excel_path", default=None, help="Path for Excel output"
    )
    parser.add_argument(
        "-gs",
        "--gsheet_auth",
        default=None,
        help="Credentials for accessing Google Sheets",
    )
    parser.add_argument(
        "-us", "--updatespreadsheets", default=None, help="Spreadsheets to update"
    )
    parser.add_argument("-sc", "--scrapers", default=None, help="Scrapers to run")
    parser.add_argument("-ut", "--updatetabs", default=None, help="Sheets to update")
    parser.add_argument(
        "-nj", "--nojson", default=False, action="store_true", help="Do not update json"
    )
    parser.add_argument(
        "-ba",
        "--basic_auths",
        default=None,
        help="Basic Auth Credentials for accessing scraper APIs",
    )
    parser.add_argument(
        "-oa",
        "--other_auths",
        default=None,
        help="Other Credentials for accessing scraper APIs",
    )
    parser.add_argument(
        "-co", "--countries_override", default=None, help="Countries to run"
    )
    parser.add_argument(
        "-sv", "--save", default=False, action="store_true", help="Save downloaded data"
    )
    parser.add_argument(
        "-usv", "--use_saved", default=False, action="store_true", help="Use saved data"
    )
    args = parser.parse_args()
    return args


def main(
    excel_path,
    gsheet_auth,
    updatesheets,
    updatetabs,
    scrapers_to_run,
    basic_auths,
    other_auths,
    nojson,
    countries_override,
    save,
    use_saved,
    **ignore,
):
    logger.info(f"##### hdx-scraper-covid-viz version {VERSION:.1f} ####")
    configuration = Configuration.read()
    with ErrorsOnExit() as errors_on_exit:
        with temp_dir() as temp_folder:
            with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
                retriever = Retrieve(
                    downloader, temp_folder, "saved_data", temp_folder, save, use_saved
                )
                if scrapers_to_run:
                    logger.info(f"Updating only scrapers: {scrapers_to_run}")
                tabs = configuration["tabs"]
                if updatetabs is None:
                    updatetabs = list(tabs.keys())
                    logger.info("Updating all tabs")
                else:
                    logger.info(f"Updating only these tabs: {updatetabs}")
                noout = NoOutput(updatetabs)
                if excel_path:
                    excelout = ExcelOutput(excel_path, tabs, updatetabs)
                else:
                    excelout = noout
                if gsheet_auth:
                    gsheets = GoogleSheets(
                        configuration, gsheet_auth, updatesheets, tabs, updatetabs
                    )
                else:
                    gsheets = noout
                if nojson:
                    jsonout = noout
                else:
                    jsonout = JsonOutput(configuration, updatetabs)
                outputs = {"gsheets": gsheets, "excel": excelout, "json": jsonout}
                today = datetime.now()
                countries_to_save, fail = get_indicators(
                    configuration,
                    today,
                    retriever,
                    outputs,
                    updatetabs,
                    scrapers_to_run,
                    basic_auths,
                    other_auths,
                    countries_override,
                    errors_on_exit,
                )
                jsonout.add_additional_json(downloader, today=today)
                jsonout.save(countries_to_save=countries_to_save)
                excelout.save()


if __name__ == "__main__":
    args = parse_args()
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = getenv("USER_AGENT")
        if user_agent is None:
            user_agent = "hdx-scraper-covid-viz"
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = getenv("PREPREFIX")
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = getenv("HDX_SITE", "prod")
    gsheet_auth = args.gsheet_auth
    if gsheet_auth is None:
        gsheet_auth = getenv("GSHEET_AUTH")
    updatesheets = args.updatespreadsheets
    if updatesheets is None:
        updatesheets = getenv("UPDATESHEETS")
    if updatesheets:
        updatesheets = updatesheets.split(",")
    else:
        updatesheets = None
    if args.updatetabs:
        updatetabs = args.updatetabs.split(",")
    else:
        updatetabs = None
    if args.scrapers:
        scrapers_to_run = args.scrapers.split(",")
    else:
        scrapers_to_run = None
    basic_auths = dict()
    ba = args.basic_auths
    if ba is None:
        ba = getenv("BASIC_AUTHS")
    if ba:
        for keyvalue in ba.split(","):
            key, value = keyvalue.split(":")
            basic_auths[key] = value
    other_auths = dict()
    oa = args.other_auths
    if oa is None:
        oa = getenv("OTHER_AUTHS")
    if oa:
        for keyvalue in oa.split(","):
            key, value = keyvalue.split(":")
            other_auths[key] = value
    if args.countries_override:
        countries_override = args.countries_override.split(",")
    else:
        countries_override = None
    facade(
        main,
        hdx_read_only=True,
        user_agent=user_agent,
        preprefix=preprefix,
        hdx_site=hdx_site,
        project_config_yaml=join("config", "project_configuration.yml"),
        excel_path=args.excel_path,
        gsheet_auth=gsheet_auth,
        updatesheets=updatesheets,
        updatetabs=updatetabs,
        scrapers_to_run=scrapers_to_run,
        basic_auths=basic_auths,
        other_auths=other_auths,
        nojson=args.nojson,
        countries_override=countries_override,
        save=args.save,
        use_saved=args.use_saved,
    )
