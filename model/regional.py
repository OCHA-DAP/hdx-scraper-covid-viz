# -*- coding: utf-8 -*-
import logging
import sys

from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import number_format, get_fraction_str, get_numeric_if_possible

logger = logging.getLogger(__name__)


def get_float_or_int(valuestr):
    if not valuestr:
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


def get_regional(configuration, national_headers, national_columns, admininfo):
    regional_config = configuration['regional']
    val_fns = regional_config['val_fns']
    headers = val_fns.keys()
    regional_headers = [list(), list()]
    regional_columns = list()
    for i, header in enumerate(national_headers[0][3:]):
        if header not in headers:
            continue
        regional_headers[0].append(header)
        regional_headers[1].append(national_headers[1][3+i])
        regional_columns.append(national_columns[i])
    valdicts = list()
    for i, header in enumerate(regional_headers[0]):
        valdict = dict()
        valdicts.append(valdict)
        action = val_fns[header]
        column = regional_columns[i]
        for countryiso in column:
            for region in admininfo.iso3_to_regions[countryiso]:
                dict_of_lists_add(valdict, region, column[countryiso])
        if action == 'sum':
            for region, valuelist in valdict.items():
                total = ''
                for valuestr in valuelist:
                    if valuestr:
                        value = get_numeric(valuestr)
                        if value:
                            if total == '':
                                total = value
                            else:
                                total += value
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
    logger.info('Processed regional')
    return regional_headers, valdicts