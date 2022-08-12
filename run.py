import argparse
import logging
from os import getenv
from os.path import join

from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.scraper.outputs.base import BaseOutput
from hdx.scraper.outputs.excelfile import ExcelFile
from hdx.scraper.outputs.googlesheets import GoogleSheets
from hdx.scraper.outputs.json import JsonFile
from hdx.scraper.utilities import string_params_to_dict
from hdx.scraper.utilities.reader import Read
from hdx.utilities.dateparse import now_utc
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import temp_dir
from scrapers.main import get_indicators

setup_logging()
logger = logging.getLogger(__name__)


VERSION = 4.0


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-hk", "--hdx_key", default=None, help="HDX api key")
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
        "-ha",
        "--header_auths",
        default=None,
        help="Header Auth Credentials for accessing scraper APIs",
    )
    parser.add_argument(
        "-ba",
        "--basic_auths",
        default=None,
        help="Basic Auth Credentials for accessing scraper APIs",
    )
    parser.add_argument(
        "-pa",
        "--param_auths",
        default=None,
        help="Extra parameters for accessing scraper APIs",
    )
    parser.add_argument(
        "-gho", "--gho_countries_override", default=None, help="GHO Countries to run"
    )
    parser.add_argument(
        "-hrp", "--hrp_countries_override", default=None, help="HRP Countries to run"
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
    header_auths,
    basic_auths,
    param_auths,
    nojson,
    gho_countries_override,
    hrp_countries_override,
    save,
    use_saved,
    **ignore,
):
    logger.info(f"##### hdx-scraper-covid-viz version {VERSION:.1f} ####")
    configuration = Configuration.read()
    with ErrorsOnExit() as errors_on_exit:
        with temp_dir() as temp_folder:
            today = now_utc()
            Read.create_readers(
                temp_folder,
                "saved_data",
                temp_folder,
                save,
                use_saved,
                hdx_auth=configuration.get_api_key(),
                header_auths=header_auths,
                basic_auths=basic_auths,
                param_auths=param_auths,
                today=today,
            )
            if scrapers_to_run:
                logger.info(f"Updating only scrapers: {scrapers_to_run}")
            tabs = configuration["tabs"]
            if updatetabs is None:
                updatetabs = list(tabs.keys())
                logger.info("Updating all tabs")
            else:
                logger.info(f"Updating only these tabs: {updatetabs}")
            noout = BaseOutput(updatetabs)
            if excel_path:
                excelout = ExcelFile(excel_path, tabs, updatetabs)
            else:
                excelout = noout
            if gsheet_auth:
                gsheets = GoogleSheets(
                    configuration["googlesheets"],
                    gsheet_auth,
                    updatesheets,
                    tabs,
                    updatetabs,
                )
            else:
                gsheets = noout
            if nojson:
                jsonout = noout
            else:
                jsonout = JsonFile(configuration["json"], updatetabs)
            outputs = {"gsheets": gsheets, "excel": excelout, "json": jsonout}
            countries_to_save = get_indicators(
                configuration,
                today,
                outputs,
                updatetabs,
                scrapers_to_run,
                gho_countries_override,
                hrp_countries_override,
                errors_on_exit,
            )
            jsonout.save(countries_to_save=countries_to_save)
            excelout.save()


if __name__ == "__main__":
    args = parse_args()
    hdx_key = args.hdx_key
    if hdx_key is None:
        hdx_key = getenv("HDX_KEY")
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
    ha = args.header_auths
    if ha is None:
        ha = getenv("HEADER_AUTHS")
    if ha:
        header_auths = string_params_to_dict(ha)
    else:
        header_auths = None
    ba = args.basic_auths
    if ba is None:
        ba = getenv("BASIC_AUTHS")
    if ba:
        basic_auths = string_params_to_dict(ba)
    else:
        basic_auths = None
    pa = args.param_auths
    if pa is None:
        pa = getenv("PARAM_AUTHS")
    if pa:
        param_auths = string_params_to_dict(pa)
    else:
        param_auths = None
    if args.gho_countries_override:
        gho_countries_override = args.gho_countries_override.split(",")
    else:
        gho_countries_override = None
    if args.hrp_countries_override:
        hrp_countries_override = args.hrp_countries_override.split(",")
    else:
        hrp_countries_override = None
    facade(
        main,
        hdx_key=hdx_key,
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
        header_auths=header_auths,
        basic_auths=basic_auths,
        param_auths=param_auths,
        nojson=args.nojson,
        gho_countries_override=gho_countries_override,
        hrp_countries_override=hrp_countries_override,
        save=args.save,
        use_saved=args.use_saved,
    )
