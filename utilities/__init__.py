# -*- coding: utf-8 -*-
import re

from hdx.data.dataset import Dataset

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


def add_population(population_lookup, headers, columns):
    if population_lookup is None:
        return
    try:
        population_index = headers[1].index('#population')
    except ValueError:
        population_index = None
    if population_index is not None:
        for key, value in columns[population_index].items():
            try:
                valint = int(value)
                population_lookup[key] = valint
            except ValueError:
                pass
