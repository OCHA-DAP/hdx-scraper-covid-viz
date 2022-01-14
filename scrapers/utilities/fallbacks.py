import logging
from os.path import join
from typing import Dict

from hdx.scraper.fallbacks import use_fallbacks
from hdx.scraper.scrapers import run_scrapers
from hdx.scraper.utils import get_sources_from_datasetinfo
from hdx.utilities.loader import LoadError, load_json

logger = logging.getLogger(__name__)


class Fallbacks:
    def __init__(
        self,
        configuration,
        gho_countries,
        adminone,
        downloader,
        basic_auths,
        today,
        today_str,
        population_lookup,
        fallbacks_root,
    ):
        self.configuration = configuration
        self.gho_countries = gho_countries
        self.adminone = adminone
        self.downloader = downloader
        self.basic_auths = basic_auths
        self.today = today
        self.today_str = today_str
        self.population_lookup = population_lookup

        fallbacks_file = configuration["json"]["additional"][0]["filepath"]
        fallbacks_path = join(fallbacks_root, fallbacks_file)
        try:
            fallback_data = load_json(fallbacks_path)
            fallback_sources = fallback_data["sources_data"]
            sources_hxltags = [
                "#indicator+name",
                "#date",
                "#meta+source",
                "#meta+url",
            ]
            self.fallbacks = {
                "global": {
                    "data": fallback_data["world_data"],
                    "admin name": "global",
                    "sources": fallback_sources,
                    "sources hxltags": sources_hxltags,
                },
                "regional": {
                    "data": fallback_data["regional_data"],
                    "admin hxltag": "#region+name",
                    "sources": fallback_sources,
                    "sources hxltags": sources_hxltags,
                },
                "national": {
                    "data": fallback_data["national_data"],
                    "admin hxltag": "#country+code",
                    "sources": fallback_sources,
                    "sources hxltags": sources_hxltags,
                },
                "subnational": {
                    "data": fallback_data["subnational_data"],
                    "admin hxltag": "#adm1+code",
                    "sources": fallback_sources,
                    "sources hxltags": sources_hxltags,
                },
            }
        except (IOError, LoadError):
            self.fallbacks = None
        self.fallbacks_used = list()

    def run_generic_scrapers(self, level, scrapers):
        scraper_configuration = self.configuration[f"scraper_{level}"]
        results = run_scrapers(
            scraper_configuration,
            level,
            self.gho_countries,
            self.adminone,
            self.downloader,
            self.basic_auths,
            today=self.today,
            today_str=self.today_str,
            scrapers=scrapers,
            population_lookup=self.population_lookup,
            fallbacks=self.fallbacks[level] if self.fallbacks else None,
        )
        fb = results.get("fallbacks")
        if fb:
            self.fallbacks_used.extend(fb)
        return results

    def with_fallbacks(
        self,
        scraper,
        scrapers_to_run,
    ):
        if scrapers_to_run and not any(x in scraper.name for x in scrapers_to_run):
            return None
        datasetinfo = self.configuration[scraper.name]
        levels = list(scraper.headers.keys())
        results = {level: {dict()} for level in levels}

        def set_results(level, vals, srcs):
            results[level]["headers"] = scraper.headers[level]
            results[level]["values"] = vals
            results[level]["sources"] = srcs

        try:
            scraper.run(datasetinfo)
            for level in levels:
                sources = get_sources_from_datasetinfo(
                    datasetinfo, scraper.headers[level][1]
                )
                set_results(level, scraper.values[level], sources)
            logger.info(f"Processed {scraper.name}")
        except Exception:
            for level in levels:
                values, sources = use_fallbacks(
                    self.fallbacks[level],
                    scraper.headers[level],
                )
                set_results(level, values, sources)
            self.fallbacks_used.append(scraper.name)
            logger.exception(f"Using fallbacks for {scraper.name}")
        return results

    def get_fallbacks_used(self):
        return self.fallbacks_used
