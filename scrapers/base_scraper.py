from abc import abstractmethod
from typing import Dict, List, Tuple

from abstractcp import Abstract, abstract_class_property


class BaseScraper(Abstract):
    name = abstract_class_property(str)
    headers = abstract_class_property(Dict[str, Tuple])

    @classmethod
    def get_headers(cls, level: str) -> Tuple[Tuple]:
        """
        Get headers for a particular level like national or subnational

        Args:
            level (str): Level for which to get headers

        Returns:
            Tuple[Tuple]: Scraper headers
        """
        return cls.headers[level]

    @abstractmethod
    def run(self, datasetinfo: Dict) -> Dict[str, Tuple]:
        """
        Run scraper and return results of the form
        {"national": ({"AFG": 1, "PSE": 3}, {"AFG": 1000, "PSE": 500}, ...))

        Args:
            datasetinfo (Dict): Information about dataset

        Returns:
            Dict[str, Tuple]: Results at each level

        """
