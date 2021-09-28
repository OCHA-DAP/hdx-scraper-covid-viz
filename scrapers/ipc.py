import inspect

from hdx.utilities.downloader import Download


def get_ipc(configuration, today, gho_countries, adminone, other_auths, scrapers=None):
    name = inspect.currentframe().f_code.co_name
    if scrapers and not any(scraper in name for scraper in scrapers):
        return list(), list(), list(), list(), list()
    ipc_configuration = configuration["ipc"]
    base_url = ipc_configuration["url"]
    with Download(
        rate_limit={"calls": 1, "period": 0.1},
        extra_params_dict={"key": other_auths["ipc"]},
    ) as downloader:
        downloader.download(f"{base_url}/analyses?type=A")
        for analysis in downloader.get_json():
            pass