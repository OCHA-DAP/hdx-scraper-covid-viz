from hdx.data.dataset import Dataset
from hdx.utilities.dateparse import parse_date


def get_monthly_report_source(configuration):
    monthly_report_configuration = configuration["monthly_report"]
    dataset = monthly_report_configuration["dataset"]
    resource = monthly_report_configuration["resource"]
    if isinstance(dataset, str):
        dataset = Dataset.read_from_hdx(dataset)
        resource_name = resource
        resource = None
        for res in dataset.get_resources():
            if res["name"] == resource_name:
                resource = res
                break
        if not resource:
            raise ValueError("No monthly report resource found!")
    last_modified = parse_date(resource["last_modified"]).strftime("%Y-%m-%d")

    return (
        monthly_report_configuration["hxltag"],
        last_modified,
        dataset["dataset_source"],
        resource["url"],
    )
