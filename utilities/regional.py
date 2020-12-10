# -*- coding: utf-8 -*-
import logging
import sys

from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import number_format, get_fraction_str, get_numeric_if_possible

from model import add_population

logger = logging.getLogger(__name__)


def get_float_or_int(valuestr):
    if not valuestr or valuestr == 'N/A':
        return None
    if '.' in valuestr:
        return float(valuestr)
    else:
        return int(valuestr)


def get_numeric(valuestr):
    if isinstance(valuestr, str):
        total = 0
        hasvalues = False
        for value in valuestr.split('|'):
            value = get_float_or_int(value)
            if value:
                hasvalues = True
                total += value
        if hasvalues is False:
            return ''
        return total
    return valuestr


def get_headers_and_columns(input_headers, input_columns, desired_headers):
    headers = [list(), list()]
    columns = list()
    for i, header in enumerate(input_headers[0]):
        if header not in desired_headers:
            continue
        headers[0].append(header)
        headers[1].append(input_headers[1][i])
        columns.append(input_columns[i])
    return headers, columns


def get_regional(configuration, regionlookup, national_headers, national_columns, population_lookup=None, *args):
    regional_config = configuration['regional']
    if population_lookup is None:
        process_cols = regional_config['process_cols']
    else:
        process_cols = {'Population': 'sum'}
    headers = process_cols.keys()
    regional_headers, regional_columns = get_headers_and_columns(national_headers, national_columns, headers)
    valdicts = list()
    for i, header in enumerate(regional_headers[0]):
        valdict = dict()
        valdicts.append(valdict)
        action = process_cols[header]
        column = regional_columns[i]
        for countryiso in column:
            for region in regionlookup.iso3_to_region_and_hrp[countryiso]:
                dict_of_lists_add(valdict, region, column[countryiso])
        if action == 'sum' or action == 'mean':
            for region, valuelist in valdict.items():
                total = ''
                novals = 0
                for valuestr in valuelist:
                    value = ''
                    if isinstance(valuestr, int) or isinstance(valuestr, float):
                        value = valuestr
                    else:
                        if valuestr:
                            value = get_numeric(valuestr)
                    if value != '':
                        novals += 1
                        if total == '':
                            total = value
                        else:
                            total += value
                if action == 'mean':
                    if not isinstance(total, str):
                        total /= novals
                if isinstance(total, float):
                    valdict[region] = number_format(total)
                else:
                    valdict[region] = total
        elif action == 'range':
            for region, valuelist in valdict.items():
                min = sys.maxsize
                max = -min
                for valuestr in valuelist:
                    if valuestr:
                        value = get_numeric(valuestr)
                        if value > max:
                            max = value
                        if value < min:
                            min = value
                if min == sys.maxsize or max == -sys.maxsize:
                    valdict[region] = ''
                else:
                    if isinstance(max, float):
                        max = number_format(max)
                    if isinstance(min, float):
                        min = number_format(min)
                    valdict[region] = '%s-%s' % (str(min), str(max))
        else:
            for region, valuelist in valdict.items():
                toeval = action
                for j in range(i):
                    value = valdicts[j].get(region, '')
                    if value == '':
                        value = None
                    toeval = toeval.replace(regional_headers[0][j], str(value))
                valdict[region] = eval(toeval)
    for arg in args:
        gheaders, gvaldicts = arg
        if gheaders:
            for i, header in enumerate(gheaders[1]):
                try:
                    j = regional_headers[1].index(header)
                except ValueError:
                    continue
                valdicts[j].update(gvaldicts[i])

    add_population(population_lookup, regional_headers, valdicts)
    logger.info('Processed regional')
    return regional_headers, valdicts


def get_world(configuration, regional_headers, regional_columns):
    desired_headers = configuration['from_regional']
    world_headers, world_columns = get_headers_and_columns(regional_headers, regional_columns, desired_headers)
    global_columns = list()
    for column in world_columns:
        global_columns.append({'global': column['H63']})
    return world_headers, global_columns



