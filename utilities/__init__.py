# -*- coding: utf-8 -*-
from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import dict_of_lists_add

from model import template


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
