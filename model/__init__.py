import logging
import re
from datetime import datetime

from hdx.data.dataset import Dataset

logger = logging.getLogger(__name__)

today = datetime.now()
today_str = today.strftime('%Y-%m-%d')
template = re.compile('{{.*?}}')


def get_percent(numerator, denominator=None):
    if denominator:
        numerator /= denominator
    return '%.2f' % numerator


def get_rowval(row, valcol):
    if '{{' in valcol:
        repvalcol = valcol
        for match in template.finditer(valcol):
            template_string = match.group()
            replace_string = 'row["%s"]' % template_string[2:-2]
            repvalcol = repvalcol.replace(template_string, replace_string)
        return eval(repvalcol)
    else:
        return row[valcol]


def get_date_from_dataset_date(dataset):
    if isinstance(dataset, str):
        dataset = Dataset.read_from_hdx(dataset)
    date_type = dataset.get_dataset_date_type()
    if date_type == 'range':
        return dataset.get_dataset_end_date(date_format='%Y-%m-%d')
    elif date_type == 'date':
        return dataset.get_dataset_date(date_format='%Y-%m-%d')
    return None

