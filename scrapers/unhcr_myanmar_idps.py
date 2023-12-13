import logging

logger = logging.getLogger(__name__)


def idps_post_run(self) -> None:
    try:
        url = self.overrideinfo["url"]
        reader = self.get_reader()
        json = reader.download_json(url, file_prefix="idps_override")
        number_idps = int(json["data"][0]["individuals"])
        index = self.get_headers("national")[1].index("#affected+displaced")
        values = self.get_values("national")[index]
        for key, current_idps in values.items():
            if key != "MMR":
                continue
            logger.info(f"Replacing {current_idps} with {number_idps} for MMR IDPs!")
            values[key] = number_idps
            self.get_source_urls().add(url)
            logger.info("Processed UNHCR Myanmar IDPs")
            break
    except Exception as ex:
        msg = "Not using UNHCR Myanmar IDPs override!"
        logger.exception(msg)
        if self.errors_on_exit:
            self.errors_on_exit.add(f"{msg} Error: {ex}")
