# -*- coding: utf-8 -*-
import logging
import re
from datetime import datetime

from hdx.utilities.text import get_fraction_str

logger = logging.getLogger(__name__)

today = datetime.now()
today_str = today.strftime('%Y-%m-%d')
template = re.compile('{{.*?}}')


def calculate_ratios(items_per_country, affected_items_per_country):
    ratios = dict()
    for countryiso in items_per_country:
        if countryiso in affected_items_per_country:
            ratios[countryiso] = get_fraction_str(affected_items_per_country[countryiso], items_per_country[countryiso])
        else:
            ratios[countryiso] = '0.0'
    return ratios


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
