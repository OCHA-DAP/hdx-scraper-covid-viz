# -*- coding: utf-8 -*-
import logging
import sys

from model import number_format, get_percent, div_100
from model.readers import read_hdx

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


def get_regional(configuration, national, admininfo):
    regional_config = configuration['regional']
    iso_index = national[1].index('#country+code')
    regional = [['regionname'], ['#region+name']]
    headers = national[0][2:]
    val_fns = regional_config['val_fns']
    header_to_valfn = dict()
    valfn_to_header = dict()
    for i, header in enumerate(val_fns):
        regional[0].append(header)
        try:
            index = headers.index(header)
            header_to_valfn[index] = i + 1
            valfn_to_header[i + 1] = index
            regional[1].append(national[1][index + 2])
        except ValueError:
            regional[1].append('')
    regions = sorted(list(admininfo.regions))
    for region in regions:
        regiondata = [region]
        regiondata.extend([list() for _ in val_fns])
        regional.append(regiondata)
    for countrydata in national[2:]:
        countryiso = countrydata[iso_index]
        for region in admininfo.iso3_to_regions.get(countryiso, list()):
            regiondata = regional[regions.index(region) + 2]
            for i, value in enumerate(countrydata[2:]):
                index = header_to_valfn.get(i)
                if index is not None:
                    regiondata[index].append(value)
    for regiondata in regional[2:]:
        for index, valuelist in enumerate(regiondata[1:]):
            action = list(val_fns.values())[index]
            if action == 'sum':
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
                    regiondata[index + 1] = number_format(total)
                else:
                    regiondata[index + 1] = total
            elif action == 'range':
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
                    regiondata[index + 1] = ''
                else:
                    if isinstance(max, float):
                        max = number_format(max)
                    if isinstance(min, float):
                        min = number_format(min)
                    regiondata[index + 1] = '%s-%s' % (str(min), str(max))
            else:
                for i, header in enumerate(val_fns):
                    value = regiondata[i + 1]
                    if value == '':
                        value = None
                    action = action.replace(header, str(value))
                regiondata[index + 1] = eval(action)
    return regional