from abc import abstractmethod
from typing import Dict, Tuple

from abstractcp import Abstract, abstract_class_property


class BaseScraper(Abstract):
    name = abstract_class_property(str)
    headers = abstract_class_property(Dict[str, Tuple])

    def __init__(self):
        """
        Create values member variable for inheriting scrapers to populate. It is of
        form: {"national": ({"AFG": 1.2, "PSE": 1.4}, {"AFG": 123, "PSE": 241}, ...)}}
        """
        self.values: Dict[str, Tuple] = {
            level: tuple(dict() for _ in value[0])
            for level, value in self.headers.items()
        }

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

    def get_values(self, level: str) -> Tuple:
        """
        Get values for a particular level like national or subnational

        Args:
            level (str): Level for which to get headers

        Returns:
            Tuple: Scraper values
        """
        return self.values[level]

    @abstractmethod
    def run(self, datasetinfo: Dict):
        """
        Run scraper

        Args:
            datasetinfo (Dict): Information about dataset

        Returns:
            None
        """
