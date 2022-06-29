from hdx.scraper.utilities.reader import Read
from hdx.utilities.dateparse import parse_date


def get_report_source(configuration):
    report_configuration = configuration["report"]
    dataset = report_configuration["dataset"]
    resource = report_configuration["resource"]
    if isinstance(dataset, str):
        dataset = Read.get_reader("report").read_dataset(dataset)
        resource_name = resource
        resource = None
        for res in dataset.get_resources():
            if res["name"] == resource_name:
                resource = res
                break
        if not resource:
            raise ValueError("No report resource found!")
    last_modified = parse_date(resource["last_modified"]).strftime("%Y-%m-%d")

    return (
        report_configuration["hxltag"],
        last_modified,
        dataset["dataset_source"],
        resource["url"],
    )
