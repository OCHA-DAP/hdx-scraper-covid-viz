from typing import Dict, List

from hdx.scraper.scrapers import use_fallbacks


def get_fallbacks(
    name: str,
    fallbacks: Dict,
    output_cols: List[str],
    output_hxltags: List[str],
):
    """Use provided fallbacks when there is a problem obtaining the latest data. The
    fallbacks dictionary should have the following keys: "data" containing a list of
    dictionaries from HXL hashtag to value, "admin name" to specify a particular admin
    name to use or "admin hxltag" specifying the HXL hashtag of the admin unit,
    "sources" containing a list of dictionaries with source information and
    "sources hxltags" containing a list of HXL hashtags with the name one first. eg.

    {"data": [{"#country+code": "AFG", "": "#value+wb+total": "572000000", ...}, ...],
    "admin hxltag": "#country+code",
    "sources": [{"#date": "2020-07-29", "#indicator+name": "#value+wb+total",
    "#meta+source": "OCHA, Center for Disaster Protection",
    "#meta+url": "https://data.humdata.org/dataset/compilation..."}, ...],
    "sources hxltags": ["#indicator+name", "#date", "#meta+source", "#meta+url"]}

    Args:
        name (str): Name of mini scraper
        levels
        fallbacks (Dict): Fallbacks dictionary
        output_cols (List[str]): Names of output columns
        output_hxltags (List[str]): HXL hashtags of output columns

    Returns:
        Tuple: Output headers, values and sources
    """
    results = dict()
    use_fallbacks(name, fallbacks, output_cols, output_hxltags, results)
    return (results["headers"], results["values"], results["sources"])

from typing import Dict, List

from hdx.scraper.scrapers import use_fallbacks


def get_fallbacks(
    name: str,
    fallbacks: Dict,
    levels: List[str],
    output_cols: Dict[str, List[str]],
    output_hxltags: Dict[str, List[str]],
):
    """Use provided fallbacks when there is a problem obtaining the latest data. The
    fallbacks dictionary should have the following keys: "data" containing a list of
    dictionaries from HXL hashtag to value, "admin name" to specify a particular admin
    name to use or "admin hxltag" specifying the HXL hashtag of the admin unit,
    "sources" containing a list of dictionaries with source information and
    "sources hxltags" containing a list of HXL hashtags with the name one first. eg.

    {"data": [{"#country+code": "AFG", "": "#value+wb+total": "572000000", ...}, ...],
    "admin hxltag": "#country+code",
    "sources": [{"#date": "2020-07-29", "#indicator+name": "#value+wb+total",
    "#meta+source": "OCHA, Center for Disaster Protection",
    "#meta+url": "https://data.humdata.org/dataset/compilation..."}, ...],
    "sources hxltags": ["#indicator+name", "#date", "#meta+source", "#meta+url"]}

    Args:
        name (str): Name of mini scraper
        fallbacks (Dict): Fallbacks dictionary
        levels
        output_cols (List[str]): Names of output columns
        output_hxltags (List[str]): HXL hashtags of output columns

    Returns:
        Tuple: Output headers, values and sources
    """
    results = dict()
    use_fallbacks(name, fallbacks, output_cols, output_hxltags, results)
    return (results["headers"], results["values"], results["sources"])
