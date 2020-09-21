# -*- coding: utf-8 -*-
import numpy
import pandas as pd

# filename for shapefile and WHO input dataset
from hdx.location.country import Country

from utilities.readers import read_hdx_metadata

MIN_CUMULATIVE_CASES = 100


def get_who_data(url, admininfo):
    df = pd.read_csv(url, keep_default_na=False)
    df.columns = df.columns.str.strip()
    df = df[['Date_reported', 'Country_code', 'Cumulative_cases', 'New_cases', 'New_deaths', 'Cumulative_deaths']]
    df.insert(1, 'ISO_3_CODE', df['Country_code'].apply(Country.get_iso3_from_iso2))
    df = df.drop(columns=['Country_code'])
    source_date = df['Date_reported'].max()

    # cumulative
    df_cumulative = df.loc[df['Date_reported'] == source_date]
    df_cumulative = df_cumulative.drop(columns=['Date_reported', 'New_cases', 'New_deaths'])
    df_world = df_cumulative.sum()
    df_cumulative = df_cumulative.loc[df['ISO_3_CODE'].isin(admininfo.countryiso3s), :]
    df_h63 = df_cumulative.sum()

    # get only HRP countries
    df = df.loc[df['ISO_3_CODE'].isin(admininfo.countryiso3s), :]

    df_series = df.loc[df['ISO_3_CODE'].isin(admininfo.hrp_iso3s), :]
    df_series = df_series.drop(columns=['New_cases', 'New_deaths'])
    df_series['CountryName'] = df_series['ISO_3_CODE'].apply(admininfo.get_country_name_from_iso3)

    df['Date_reported'] = pd.to_datetime(df['Date_reported'])

    # adding global H63 by date
    df_h63_all = df.groupby('Date_reported').sum()
    df_h63_all['ISO_3_CODE'] = 'H63'
    df_h63_all = df_h63_all.reset_index()

    # adding global H25 by date
    df_h25_all = df.loc[df['ISO_3_CODE'].isin(admininfo.hrp_iso3s), :]
    df_h25_all = df_h25_all.groupby('Date_reported').sum()
    df_h25_all['ISO_3_CODE'] = 'H25'
    df_h25_all = df_h25_all.reset_index()

    # adding regional by date
    dict_regions = pd.DataFrame(admininfo.iso3_to_region.items(), columns=['ISO3', 'Regional_office'])
    df = pd.merge(left=df, right=dict_regions, left_on='ISO_3_CODE', right_on='ISO3', how='left')
    df = df.drop(labels='ISO3', axis='columns')
    df_regional = df.groupby(['Date_reported', 'Regional_office']).sum().reset_index()
    df_regional = df_regional.rename(columns={'Regional_office': 'ISO_3_CODE'})

    df = df.append(df_h63_all)
    df = df.append(df_h25_all)
    df = df.append(df_regional)

    return source_date, df_world, df_h63, df_cumulative, df_series, df


def get_who_covid(configuration, outputs, admininfo, population_lookup, scrapers=None):
    name = 'who_covid'
    if scrapers and not any(scraper in name for scraper in scrapers) and not any(scraper in outputs['gsheets'].updatetabs for scraper in scrapers):
        return list()
    datasetinfo = configuration[name]
    read_hdx_metadata(datasetinfo)

    # get WHO data
    source_date, df_world, df_h63, df_cumulative, df_series, df_WHO = get_who_data(datasetinfo['url'], admininfo)

    # output time series
    series_hxltags = {'ISO_3_CODE': '#country+code', 'CountryName': '#country+name', 'Date_reported': '#date+reported', 'Cumulative_cases': '#affected+infected', 'Cumulative_deaths': '#affected+killed'}
    series_name = 'covid_series'
    outputs['gsheets'].update_tab(series_name, df_series, series_hxltags, 1000)  # 1000 rows in gsheets!
    outputs['excel'].update_tab(series_name, df_series, series_hxltags)
    json_df = df_series.groupby('CountryName').apply(lambda x: x.to_dict('r'))
    del series_hxltags['CountryName']
    for rows in json_df:
        countryiso = rows[0]['CountryName']
        outputs['json'].add_data_rows_by_key(series_name, countryiso, rows, series_hxltags)

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
    trend_hxltags = {'ISO_3_CODE': '#country+code', 'Date_reported': '#date+reported', 'weekly_new_cases': '#affected+infected+new+weekly', 'weekly_new_deaths': '#affected+killed+new+weekly', 'weekly_new_cases_per_ht': '#affected+infected+new+per100000+weekly', 'weekly_new_cases_pc_change': '#affected+infected+new+pct+weekly'}
    outputs['gsheets'].update_tab(name, output_df, trend_hxltags)
    outputs['excel'].update_tab(name, output_df, trend_hxltags)
    # Save as JSON
    json_df = output_df.replace([numpy.inf, -numpy.inf], '').groupby('ISO_3_CODE').apply(lambda x: x.to_dict('r'))
    del trend_hxltags['ISO_3_CODE']
    for rows in json_df:
        countryiso = rows[0]['ISO_3_CODE']
        outputs['json'].add_data_rows_by_key(name, countryiso, rows, trend_hxltags)
    hxltags = set(series_hxltags.values()) | set(trend_hxltags.values())
    hxltags.remove('#country+code')
    hxltags.remove('#date+reported')
    dssource = datasetinfo['source']
    dssourceurl = datasetinfo['source_url']
    sources = [(hxltag, source_date, dssource, dssourceurl) for hxltag in hxltags]

    hxltags = ['#affected+infected', '#affected+killed']
    headers = [['Covid Cases', 'Covid Deaths'], hxltags]
    global_cases = {'global': int(df_world['Cumulative_cases'])}
    global_deaths = {'global': int(df_world['Cumulative_deaths'])}
    h63_cases = {'global': int(df_h63['Cumulative_cases'])}
    h63_deaths = {'global': int(df_h63['Cumulative_deaths'])}
    national_cases = dict(zip(df_cumulative['ISO_3_CODE'], df_cumulative['Cumulative_cases'].astype(int)))
    national_deaths = dict(zip(df_cumulative['ISO_3_CODE'], df_cumulative['Cumulative_deaths'].astype(int)))
    sources.extend([(hxltag, source_date, dssource, dssourceurl) for hxltag in hxltags])
    return headers, [global_cases, global_deaths], [h63_cases, h63_deaths], [national_cases, national_deaths], sources
