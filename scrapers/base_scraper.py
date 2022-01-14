from abc import abstractmethod
from typing import Dict, Tuple

from abstractcp import Abstract, abstract_class_property


class BaseScraper(Abstract):
    name = abstract_class_property(str)
    headers = abstract_class_property(Dict[str, Tuple])

    def __init__(self):
        self.values = {level: (dict() for _ in value[0]) for level, value in self.headers.items()}

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

    def get_values(self, level: str) -> Dict[str, Tuple]:
        """
        Get values for a particular level like national or subnational

        Args:
            level (str): Level for which to get headers

        Returns:
            Dict[str, Tuple]: Scraper headers
        """
        return self.values[level]

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
