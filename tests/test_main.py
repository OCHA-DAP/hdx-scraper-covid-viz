import filecmp
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.scraper.outputs.base import BaseOutput
from hdx.scraper.outputs.json import JsonFile
from hdx.scraper.utilities.fallbacks import Fallbacks
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from scrapers.main import get_indicators


class TestCovid:
    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            user_agent="test",
            project_config_yaml=join("tests", "config", "project_configuration.yml"),
        )
        Locations.set_validlocations(
            [
                {"name": "afg", "title": "Afghanistan"},
                {"name": "pse", "title": "State of Palestine"},
            ]
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def folder(self):
        return join("tests", "fixtures")

    def test_get_indicators(self, configuration, folder):
        with temp_dir(
            "TestCovidViz", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader, tempdir, folder, tempdir, save=False, use_saved=True
                )
                tabs = configuration["tabs"]
                noout = BaseOutput(tabs)
                jsonout = JsonFile(configuration["json"], tabs)
                outputs = {"gsheets": noout, "excel": noout, "json": jsonout}
                today = parse_date("2021-05-03")
                countries_to_save = get_indicators(
                    configuration,
                    today,
                    retriever,
                    outputs,
                    tabs,
                    scrapers_to_run=[
                        "population_national",
                        "ifi",
                        "worldhealthorg_national",
                        "worldhealthorg2_national",
                        "worldhealthorg_subnational",
                        "who_covid",
                        "sadd",
                        "covidtests",
                        "cadre_harmonise",
                        "access_national",
                        "food_prices",
                    ],
                    use_live=False,
                )
                filepaths = jsonout.save(tempdir, countries_to_save=countries_to_save)
                assert filecmp.cmp(filepaths[0], join(folder, "test_scraper_all.json"))
                assert filecmp.cmp(filepaths[1], join(folder, "test_scraper.json"))
                assert filecmp.cmp(
                    filepaths[2], join(folder, "test_scraper_daily.json")
                )
                assert filecmp.cmp(
                    filepaths[3], join(folder, "test_scraper_covidseries.json")
                )

    def test_fallbacks(self, configuration, folder):
        with temp_dir(
            "TestCovidVizFB", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader, tempdir, folder, tempdir, save=False, use_saved=True
                )
                tabs = configuration["tabs"]
                noout = BaseOutput(tabs)
                json_configuration = configuration["json"]
                jsonout = JsonFile(json_configuration, tabs)
                outputs = {"gsheets": noout, "excel": noout, "json": jsonout}
                today = parse_date("2021-05-03")
                fallbacks_path = join(folder, json_configuration["filepath"])
                levels_mapping = {
                    "global": "world_data",
                    "regional": "regional_data",
                    "national": "national_data",
                    "subnational": "subnational_data",
                }
                Fallbacks.add(
                    fallbacks_path,
                    levels_mapping=levels_mapping,
                    sources_key="sources_data",
                )
                countries_to_save = get_indicators(
                    configuration,
                    today,
                    retriever,
                    outputs,
                    tabs,
                    scrapers_to_run=[
                        "population_national",
                        "ifi",
                        "worldhealthorg_national",
                        "worldhealthorg2_national",
                        "worldhealthorg_subnational",
                        "who_covid",
                        "sadd",
                        "ctfallbacks",
                        "cadre_harmonise",
                        "access_national",
                        "food_prices",
                    ],
                    use_live=False,
                )
                filepaths = jsonout.save(tempdir, countries_to_save=countries_to_save)
                assert filecmp.cmp(filepaths[0], join(folder, "test_scraper_all.json"))
                assert filecmp.cmp(filepaths[1], join(folder, "test_scraper.json"))
                assert filecmp.cmp(
                    filepaths[2], join(folder, "test_scraper_daily.json")
                )
                assert filecmp.cmp(
                    filepaths[3], join(folder, "test_scraper_covidseries.json")
                )
