# -*- coding: utf-8 -*-
import numpy
import pandas as pd

# filename for shapefile and WHO input dataset
from model import today_str
from utilities.readers import read_hdx_metadata

MIN_CUMULATIVE_CASES = 100


def get_WHO_data(url, admininfo):
    df = pd.read_csv(url, usecols=['date_epicrv', 'ISO_3_CODE', 'CumCase', 'NewCase', 'NewDeath', 'CumDeath'])
    # get only HRP countries
    df = df.loc[df['ISO_3_CODE'].isin(admininfo.countryiso3s), :]
    df['date_epicrv'] = pd.to_datetime(df['date_epicrv'])

    # adding global by date
    df_H63 = df.groupby('date_epicrv').sum()
    df_H63['ISO_3_CODE'] = 'H63'
    df_H63 = df_H63.reset_index()

    # adding global H25 by date
    df_H25 = df.loc[df['ISO_3_CODE'].isin(admininfo.hrp_iso3s), :]
    df_H25 = df_H25.groupby('date_epicrv').sum()
    df_H25['ISO_3_CODE'] = 'H25'
    df_H25 = df_H25.reset_index()

    # adding regional by date
    dict_regions = pd.DataFrame(admininfo.iso3_to_region.items(), columns=['ISO3', 'Regional_office'])
    df = pd.merge(left=df, right=dict_regions, left_on='ISO_3_CODE', right_on='ISO3', how='left')
    df = df.drop(labels='ISO3', axis='columns')
    df_regional = df.groupby(['date_epicrv', 'Regional_office']).sum().reset_index()
    df_regional = df_regional.rename(columns={'Regional_office': 'ISO_3_CODE'})

    df = df.append(df_H63)
    df = df.append(df_H25)
    df = df.append(df_regional)
    return df


def get_covid_trend(configuration, gsheets, jsonout, admininfo, population_lookup, scrapers=None):
    name = 'covid_trend'
    if scrapers and not any(scraper in name for scraper in scrapers) and name not in gsheets.updatetabs:
        return list()
    datasetinfo = configuration[name]
    read_hdx_metadata(datasetinfo)

    # get WHO data and calculate sum as 'H63'
    df_WHO = get_WHO_data(datasetinfo['url'], admininfo)

    # get weekly new cases
    new_w = df_WHO.groupby(['ISO_3_CODE']).resample('W', on='date_epicrv').sum()[['NewCase', 'NewDeath']]
    cumulative_w = df_WHO.groupby(['ISO_3_CODE']).resample('W', on='date_epicrv').min()[['CumCase', 'CumDeath']]
    ndays_w = df_WHO.groupby(['ISO_3_CODE']).resample('W', on='date_epicrv').count()['NewCase']
    ndays_w = ndays_w.rename('ndays')

    output_df = pd.merge(left=new_w, right=cumulative_w, left_index=True, right_index=True, how='inner')
    output_df = pd.merge(left=output_df, right=ndays_w, left_index=True, right_index=True, how='inner')
    output_df = output_df[output_df['ndays'] == 7]
    output_df = output_df.reset_index()

    output_df['NewCase_PercentChange'] = output_df.groupby('ISO_3_CODE')['NewCase'].pct_change()
    output_df['NewDeath_PercentChange'] = output_df.groupby('ISO_3_CODE')['NewDeath'].pct_change()
    # For percent change, if the diff is actually 0, change nan to 0
    output_df['diff_cases'] = output_df.groupby('ISO_3_CODE')['NewCase'].diff()
    output_df.loc[
        (output_df['NewCase_PercentChange'].isna()) & (output_df['diff_cases'] == 0), 'NewCase_PercentChange'] = 0.0
    output_df['diff_deaths'] = output_df.groupby('ISO_3_CODE')['NewDeath'].diff()
    output_df.loc[
        (output_df['NewDeath_PercentChange'].isna()) & (output_df['diff_deaths'] == 0), 'NewDeath_PercentChange'] = 0.0

    output_df = output_df[output_df['CumCase'] > MIN_CUMULATIVE_CASES]

    df_pop = pd.DataFrame.from_records(list(population_lookup.items()), columns=['Country Code', 'population'])

    # Add pop to output df
    output_df = output_df.merge(df_pop, left_on='ISO_3_CODE', right_on='Country Code', how='left').drop(
        columns=['Country Code'])
    # Get cases per hundred thousand
    output_df = output_df.rename(columns={'NewCase': 'weekly_new_cases', 'NewDeath': 'weekly_new_deaths',
                                          'CumCase': 'cumulative_cases', 'CumDeath': 'cumulative_deaths'})
    output_df['weekly_new_cases_per_ht'] = output_df['weekly_new_cases'] / output_df['population'] * 1E5
    output_df['weekly_new_deaths_per_ht'] = output_df['weekly_new_deaths'] / output_df['population'] * 1E5
    output_df['weekly_new_cases_pc_change'] = output_df['NewCase_PercentChange'] * 100
    output_df['weekly_new_deaths_pc_change'] = output_df['NewDeath_PercentChange'] * 100

    # Save as JSON
    output_df['date_epicrv'] = output_df['date_epicrv'].apply(lambda x: x.strftime('%Y-%m-%d'))
    output_df = output_df.drop(
        ['NewCase_PercentChange', 'NewDeath_PercentChange', 'ndays', 'diff_cases', 'diff_deaths'], axis=1)
    gsheets.update_tab(name, output_df)
    json_df = output_df.replace([numpy.inf, -numpy.inf], '').groupby('ISO_3_CODE').apply(lambda x: x.to_dict('r'))
    for rows in json_df:
        countryiso = rows[0]['ISO_3_CODE']
        jsonout.add_data_rows_by_key(name, countryiso, rows)
    return [(datasetinfo['hxltag'], today_str, datasetinfo['source'], datasetinfo['source_url'])]
