# -*- coding: utf-8 -*-
import numpy
import pandas as pd



# filename for shapefile and WHO input dataset
from hdx.location.country import Country
from hdx.scraper.readers import read_hdx_metadata

MIN_CUMULATIVE_CASES = 100


def get_who_data(url, h25, h63, region):
    df = pd.read_csv(url, keep_default_na=False)
    df.columns = df.columns.str.strip()
    df = df[['Date_reported', 'Country_code', 'Cumulative_cases', 'New_cases', 'New_deaths', 'Cumulative_deaths']]
    df.insert(1, 'ISO_3_CODE', df['Country_code'].apply(Country.get_iso3_from_iso2))
    df = df.drop(columns=['Country_code'])
    source_date = df['Date_reported'].max()

    # cumulative
    df_cumulative = df.sort_values(by=['Date_reported']).drop_duplicates(subset='ISO_3_CODE', keep='last')
    df_cumulative = df_cumulative.drop(columns=['Date_reported', 'New_cases', 'New_deaths'])
    df_world = df_cumulative.sum()
    df_cumulative = df_cumulative.loc[df['ISO_3_CODE'].isin(h63), :]
    df_h63 = df_cumulative.sum()

    df = df.loc[df['ISO_3_CODE'].isin(h63), :]

    df_series = df.copy(deep=True)  # used in series processing, keeps df unchanged for use elsewhere
    df_series['CountryName'] = df_series['ISO_3_CODE'].apply(Country.get_country_name_from_iso3)  # goes on to be output as covid series tab

    df['Date_reported'] = pd.to_datetime(df['Date_reported'])

    # adding global H63 by date
    df_h63_all = df.groupby('Date_reported').sum()
    df_h63_all['ISO_3_CODE'] = 'H63'
    df_h63_all = df_h63_all.reset_index()

    # adding global H25 by date
    df_h25_all = df.loc[df['ISO_3_CODE'].isin(h25), :]
    df_h25_all = df_h25_all.groupby('Date_reported').sum()
    df_h25_all['ISO_3_CODE'] = 'H25'
    df_h25_all = df_h25_all.reset_index()

    # adding regional by date
    dict_regions = pd.DataFrame(region.iso3_to_region.items(), columns=['ISO3', 'Regional_office'])
    df = pd.merge(left=df, right=dict_regions, left_on='ISO_3_CODE', right_on='ISO3', how='left')
    df = df.drop(labels='ISO3', axis='columns')
    df_regional = df.groupby(['Date_reported', 'Regional_office']).sum().reset_index()
    df_regional = df_regional.rename(columns={'Regional_office': 'ISO_3_CODE'})

    df = df.append(df_h63_all)
    df = df.append(df_h25_all)
    df = df.append(df_regional)

    return source_date, df_world, df_h63, df_series, df


def get_who_covid(configuration, today, outputs, h25, h63, region, population_lookup, scrapers=None):
    name = 'who_covid'
    if scrapers and not any(scraper in name for scraper in scrapers) and not any(scraper in outputs['gsheets'].updatetabs for scraper in scrapers):
        return list(), list(), list(), list(), list(), list()
    datasetinfo = configuration[name]
    read_hdx_metadata(datasetinfo, today=today)

    # get WHO data
    source_date, df_world, df_h63, df_series, df_WHO = get_who_data(datasetinfo['url'], h25, h63, region)
    df_pop = pd.DataFrame.from_records(list(population_lookup.items()), columns=['Country Code', 'population'])

    # output time series
    series_headers = ['Cumulative_cases', 'Average_cases_7_days', 'Average_cases_7_days_per_100000',
                      'Average_cases_7_days_per_100000_pc_change', 'Cumulative_deaths', 'Average_deaths_7_days',
                      'Average_deaths_7_days_pc_change']
    series_hxltags = ['#affected+infected', '#affected+infected+avg', '#affected+infected+avg+per100000',
                      '#affected+infected+avg+per100000+pct+change', '#affected+killed', '#affected+killed+avg',
                      '#affected+killed+avg+pct+change']
    series_headers_hxltags = {'ISO_3_CODE': '#country+code',
                              'CountryName': '#country+name',
                              'Date_reported': '#date+reported'}
    for i, header in enumerate(series_headers):
        series_headers_hxltags[header] = series_hxltags[i]
    series_name = 'covid_series'
    df_series['Average_cases_7_days'] = df_series.groupby('ISO_3_CODE')['New_cases'].rolling(window=7, min_periods=1).mean().reset_index(0, drop=True)
    df_series['Average_deaths_7_days'] = df_series.groupby('ISO_3_CODE')['New_deaths'].rolling(window=7, min_periods=1).mean().reset_index(0, drop=True)
    df_series = df_series.merge(df_pop, left_on='ISO_3_CODE', right_on='Country Code', how='left').drop(columns=['Country Code'])
    df_series['Average_cases_7_days_per_100000'] = df_series['Average_cases_7_days'] / df_series['population'] * 100000
    df_series = df_series.drop(['New_cases', 'New_deaths', 'population'], axis=1)

    # get daily trends in cases (per 100k, 7 day avg) and deaths (7 day average); added for trend arrows in PDF
    df_series['Average_cases_7_days_per_100000_pc_change'] = df_series.groupby('ISO_3_CODE')[
        'Average_cases_7_days_per_100000'].pct_change() * 100
    df_series['Average_cases_7_days_per_100000_pc_change'].fillna(value=0, inplace=True)  # nan values are handled, but inf values remain in output
    df_series['Average_cases_7_days_per_100000_pc_change'] = df_series['Average_cases_7_days_per_100000_pc_change'].replace([numpy.inf, -numpy.inf],'inf')
    df_series['Average_deaths_7_days_pc_change'] = df_series.groupby('ISO_3_CODE')[
        'Average_deaths_7_days'].pct_change() * 100
    df_series['Average_deaths_7_days_pc_change'].fillna(value=0, inplace=True)
    df_series['Average_deaths_7_days_pc_change'] = df_series['Average_deaths_7_days_pc_change'].replace([numpy.inf, -numpy.inf], 'inf')

    outputs['gsheets'].update_tab(series_name, df_series, series_headers_hxltags, 1000)  # 1000 rows in gsheets!/;
    outputs['excel'].update_tab(series_name, df_series, series_headers_hxltags)
    outputs['json'].update_tab('covid_series_flat', df_series, series_headers_hxltags)
    json_df = df_series.groupby('CountryName').apply(lambda x: x.to_dict('r'))
    del series_headers_hxltags['CountryName']  # prevents it from being output as it is already the key
    for rows in json_df:
        countryname = rows[0]['CountryName']
        outputs['json'].add_data_rows_by_key(series_name, countryname, rows, series_headers_hxltags)

    df_cumulative = df_series.sort_values(by=['Date_reported']).drop_duplicates(subset='ISO_3_CODE', keep='last')
    national_columns = list()
    for header in series_headers:
        if 'Cumulative' in header:
            format = '%.0f'
        else:
            format = '%.4f'

        def format_number(x):
            if isinstance(x, str):
                return x
            return format % x

        national_columns.append(dict(zip(df_cumulative['ISO_3_CODE'], df_cumulative[header].map(format_number))))

    # get weekly new cases - for old covid viz output
    resampled = df_WHO.groupby(['ISO_3_CODE']).resample('W', on='Date_reported')
    new_w = resampled.sum()[['New_cases', 'New_deaths']]
    cumulative_w = resampled.min()[['Cumulative_cases', 'Cumulative_deaths']]
    ndays_w = resampled.count()['New_cases']
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

    # comment out to include PRK
    # output_df = output_df[output_df['Cumulative_cases'] > MIN_CUMULATIVE_CASES]

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
    trend_name = 'covid_trend'
    outputs['gsheets'].update_tab(trend_name, output_df, trend_hxltags)
    outputs['excel'].update_tab(trend_name, output_df, trend_hxltags)
    # Save as JSON
    json_df = output_df.replace([numpy.inf, -numpy.inf, numpy.nan], '').groupby('ISO_3_CODE').apply(lambda x: x.to_dict('r'))
    del trend_hxltags['ISO_3_CODE']
    for rows in json_df:
        countryiso = rows[0]['ISO_3_CODE']
        outputs['json'].add_data_rows_by_key(name, countryiso, rows, trend_hxltags)
    hxltags = set(series_hxltags) | set(trend_hxltags.values())
    hxltags.remove('#date+reported')
    dssource = datasetinfo['source']
    dssourceurl = datasetinfo['source_url']
    sources = [(hxltag, source_date, dssource, dssourceurl) for hxltag in sorted(hxltags)]

    headers = [['Cumulative_cases', 'Cumulative_deaths'], ['#affected+infected', '#affected+killed']]
    national_headers = [series_headers, series_hxltags]
    global_cases = {'global': int(df_world['Cumulative_cases'])}
    global_deaths = {'global': int(df_world['Cumulative_deaths'])}
    h63_cases = {'global': int(df_h63['Cumulative_cases'])}
    h63_deaths = {'global': int(df_h63['Cumulative_deaths'])}
    sources.extend([(hxltag, source_date, dssource, dssourceurl) for hxltag in series_hxltags])
    return headers, [global_cases, global_deaths], [h63_cases, h63_deaths], \
           national_headers, national_columns, sources

