from os.path import join

import pytest
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.path import temp_dir

from model.main import get_indicators


class TestCovid:
    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}, {'name': 'pse', 'title': 'State of Palestine'}])
        Country.countriesdata(use_live=False)

    @pytest.fixture(scope='function')
    def folder(self):
        return join('tests', 'fixtures')

    def test_get_indicators(self, configuration, folder):
        return
