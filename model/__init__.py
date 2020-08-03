import logging
import re
from datetime import datetime

from hdx.data.dataset import Dataset
from hdx.utilities.text import get_fraction_str

logger = logging.getLogger(__name__)

today = datetime.now()
today_str = today.strftime('%Y-%m-%d')
template = re.compile('{{.*?}}')


def get_rowval(row, valcol):
    if '{{' in valcol:
        repvalcol = valcol
        for match in template.finditer(valcol):
            template_string = match.group()
            replace_string = 'row["%s"]' % template_string[2:-2]
            repvalcol = repvalcol.replace(template_string, replace_string)
        return eval(repvalcol)
    else:
        result = row[valcol]
        if isinstance(result, str):
            return result.strip()
        return result


def get_date_from_dataset_date(dataset):
    if isinstance(dataset, str):
        dataset = Dataset.read_from_hdx(dataset)
    date_type = dataset.get_dataset_date_type()
    if date_type == 'range':
        return dataset.get_dataset_end_date(date_format='%Y-%m-%d')
    elif date_type == 'date':
        return dataset.get_dataset_date(date_format='%Y-%m-%d')
    return None


def calculate_ratios(items_per_country, affected_items_per_country):
    ratios = dict()
    for countryiso in items_per_country:
        if countryiso in affected_items_per_country:
            ratios[countryiso] = get_fraction_str(affected_items_per_country[countryiso], items_per_country[countryiso])
        else:
            ratios[countryiso] = '0.0'
    return ratios
