import filecmp
from os.path import join

import pytest
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.scraper.jsonoutput import JsonOutput
from hdx.scraper.nooutput import NoOutput
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

from model.main import get_indicators


class TestCovid:
    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, hdx_site='prod', user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}, {'name': 'pse', 'title': 'State of Palestine'}])
        return Configuration.read()

    @pytest.fixture(scope='function')
    def folder(self):
        return join('tests', 'fixtures')

    def test_get_indicators(self, configuration, folder):
        with temp_dir('TestCovidViz', delete_on_success=True, delete_on_failure=False) as tempdir:
            with Download(user_agent='test') as downloader:
                tabs = configuration['tabs']
                noout = NoOutput(tabs)
                jsonout = JsonOutput(configuration, tabs)
                outputs = {'gsheets': noout, 'excel': noout, 'json': jsonout}
                today = parse_date('2020-10-01')
                countries_to_save = get_indicators(configuration, today, downloader, outputs, tabs, scrapers=['ifi', 'who_global', 'who_national', 'who_subnational', 'who_covid', 'sadd', 'covidtests', 'cadre_harmonise', 'access'], use_live=False)
                filepaths = jsonout.save(tempdir, countries_to_save=countries_to_save)
                assert filecmp.cmp(filepaths[0], join(folder, 'test_scraper_all.json'))
                assert filecmp.cmp(filepaths[1], join(folder, 'test_scraper.json'))
                assert filecmp.cmp(filepaths[2], join(folder, 'test_scraper_daily.json'))
                assert filecmp.cmp(filepaths[3], join(folder, 'test_scraper_covidseries.json'))
