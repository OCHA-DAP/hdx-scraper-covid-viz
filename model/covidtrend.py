# -*- coding: utf-8 -*-
import numpy
import pandas as pd

# filename for shapefile and WHO input dataset
from hdx.location.country import Country

from model import today_str
from utilities.readers import read_hdx_metadata

MIN_CUMULATIVE_CASES = 100


def get_WHO_data(url, admininfo):
    df = pd.read_csv(url, keep_default_na=False)
    df.columns = df.columns.str.strip()
    df = df[['Date_reported', 'Country_code', 'Cumulative_cases', 'New_cases', 'New_deaths', 'Cumulative_deaths']]
    df['ISO_3_CODE'] = df['Country_code'].apply(Country.get_iso3_from_iso2)
    df.drop(columns=['Country_code'])
    # get only HRP countries
    df = df.loc[df['ISO_3_CODE'].isin(admininfo.countryiso3s), :]
    df['Date_reported'] = pd.to_datetime(df['Date_reported'])

    # adding global by date
    df_H63 = df.groupby('Date_reported').sum()
    df_H63['ISO_3_CODE'] = 'H63'
    df_H63 = df_H63.reset_index()

    # adding global H25 by date
    df_H25 = df.loc[df['ISO_3_CODE'].isin(admininfo.hrp_iso3s), :]
    df_H25 = df_H25.groupby('Date_reported').sum()
    df_H25['ISO_3_CODE'] = 'H25'
    df_H25 = df_H25.reset_index()

    # adding regional by date
    dict_regions = pd.DataFrame(admininfo.iso3_to_region.items(), columns=['ISO3', 'Regional_office'])
    df = pd.merge(left=df, right=dict_regions, left_on='ISO_3_CODE', right_on='ISO3', how='left')
    df = df.drop(labels='ISO3', axis='columns')
    df_regional = df.groupby(['Date_reported', 'Regional_office']).sum().reset_index()
    df_regional = df_regional.rename(columns={'Regional_office': 'ISO_3_CODE'})

    df = df.append(df_H63)
    df = df.append(df_H25)
    df = df.append(df_regional)
    return df


def get_covid_trend(configuration, outputs, admininfo, population_lookup, scrapers=None):
    name = 'covid_trend'
    if scrapers and not any(scraper in name for scraper in scrapers) and not any(scraper in outputs['gsheets'].updatetabs for scraper in scrapers):
        return list()
    datasetinfo = configuration[name]
    read_hdx_metadata(datasetinfo)

    # get WHO data and calculate sum as 'H63'
    df_WHO = get_WHO_data(datasetinfo['url'], admininfo)
    source_date = max(df_WHO['Date_reported']).strftime('%Y-%m-%d')

    # get weekly new cases
    new_w = df_WHO.groupby(['ISO_3_CODE']).resample('W', on='Date_reported').sum()[['New_cases', 'New_deaths']]
    cumulative_w = df_WHO.groupby(['ISO_3_CODE']).resample('W', on='Date_reported').min()[['Cumulative_cases', 'Cumulative_deaths']]
    ndays_w = df_WHO.groupby(['ISO_3_CODE']).resample('W', on='Date_reported').count()['New_cases']
    ndays_w = ndays_w.rename('ndays')

    output_df = pd.merge(left=new_w, right=cumulative_w, left_index=True, right_index=True, how='inner')
    output_df = pd.merge(left=output_df, right=ndays_w, left_index=True, right_index=True, how='inner')
    output_df = output_df[output_df['ndays'] == 7]
    output_df = output_df.reset_index()

    output_df['NewCase_PercentChange'] = output_df.groupby('ISO_3_CODE')['New_cases'].pct_change()
    output_df['NewDeath_PercentChange'] = output_df.groupby('ISO_3_CODE')['New_deaths'].pct_change()
    # For percent change, if the diff is actually 0, change nan to 0
    output_df['diff_cases'] = output_df.groupby('ISO_3_CODE')['New_cases'].diff()
    output_df.loc[
        (output_df['NewCase_PercentChange'].isna()) & (output_df['diff_cases'] == 0), 'NewCase_PercentChange'] = 0.0
    output_df['diff_deaths'] = output_df.groupby('ISO_3_CODE')['New_deaths'].diff()
    output_df.loc[
        (output_df['NewDeath_PercentChange'].isna()) & (output_df['diff_deaths'] == 0), 'NewDeath_PercentChange'] = 0.0

    output_df = output_df[output_df['Cumulative_cases'] > MIN_CUMULATIVE_CASES]

    df_pop = pd.DataFrame.from_records(list(population_lookup.items()), columns=['Country Code', 'population'])

    # Add pop to output df
    output_df = output_df.merge(df_pop, left_on='ISO_3_CODE', right_on='Country Code', how='left').drop(
        columns=['Country Code'])
    # Get cases per hundred thousand
    output_df = output_df.rename(columns={'New_cases': 'weekly_new_cases', 'New_deaths': 'weekly_new_deaths',
                                          'Cumulative_cases': 'cumulative_cases', 'Cumulative_deaths': 'cumulative_deaths'})
    output_df['weekly_new_cases_per_ht'] = output_df['weekly_new_cases'] / output_df['population'] * 1E5
    output_df['weekly_new_deaths_per_ht'] = output_df['weekly_new_deaths'] / output_df['population'] * 1E5
    output_df['weekly_new_cases_pc_change'] = output_df['NewCase_PercentChange'] * 100
    output_df['weekly_new_deaths_pc_change'] = output_df['NewDeath_PercentChange'] * 100

    output_df['Date_reported'] = output_df['Date_reported'].apply(lambda x: x.strftime('%Y-%m-%d'))
    output_df = output_df.drop(
        ['NewCase_PercentChange', 'NewDeath_PercentChange', 'ndays', 'diff_cases', 'diff_deaths'], axis=1)
    outputs['gsheets'].update_tab(name, output_df)
    outputs['excel'].update_tab(name, output_df)
    # Save as JSON
    json_df = output_df.replace([numpy.inf, -numpy.inf], '').groupby('ISO_3_CODE').apply(lambda x: x.to_dict('r'))
    for rows in json_df:
        countryiso = rows[0]['ISO_3_CODE']
        outputs['json'].add_data_rows_by_key(name, countryiso, rows)
    return [(datasetinfo['hxltag'], source_date, datasetinfo['source'], datasetinfo['source_url'])]
