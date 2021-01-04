# -*- coding: utf-8 -*-
import re

from hdx.data.dataset import Dataset

template = re.compile('{{.*?}}')


def match_template(input):
    match = template.search(input)
    if match:
        template_string = match.group()
        return template_string, template_string[2:-2]
    return None, None


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


def get_date_from_dataset_date(dataset, today=None):
    if isinstance(dataset, str):
        dataset = Dataset.read_from_hdx(dataset)
    if today is None:
        date_info = dataset.get_date_of_dataset()
    else:
        date_info = dataset.get_date_of_dataset(today=today)
    enddate = date_info.get('enddate_str')
    if not enddate:
        return None
    return enddate[:10]


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
