import logging
from copy import deepcopy

import numpy
import pandas as pd
from hdx.location.country import Country
from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.utilities.readers import read_hdx_metadata
from hdx.utilities.text import number_format

logger = logging.getLogger(__name__)


class WHOCovid(BaseScraper):
    def __init__(
        self,
        datasetinfo,
        today,
        outputs,
        hrp_countries,
        gho_countries,
        iso3_to_region,
        population_lookup,
    ):
        base_headers = ["Cumulative_cases", "Cumulative_deaths"]
        base_hxltags = ["#affected+infected", "#affected+killed"]
        self.trend_hxltags = {
            "ISO_3_CODE": "#country+code",
            "Date_reported": "#date+reported",
            "weekly_cum_cases": "#affected+infected+cumulative+weekly",
            "weekly_cum_deaths": "#affected+killed+cumulative+weekly",
            "weekly_new_cases": "#affected+infected+new+weekly",
            "weekly_new_deaths": "#affected+killed+new+weekly",
            "weekly_new_cases_per_ht": "#affected+infected+new+per100000+weekly",
            "weekly_new_deaths_per_ht": "#affected+killed+new+per100000+weekly",
            "weekly_new_cases_change": "#affected+infected+new+change+weekly",
            "weekly_new_deaths_change": "#affected+killed+new+change+weekly",
            "weekly_new_cases_pc_change": "#affected+infected+new+pct+weekly",
            "weekly_new_deaths_pc_change": "#affected+killed+new+pct+weekly",
        }
        self.weekly_hxltags = deepcopy(self.trend_hxltags)
        del self.weekly_hxltags["ISO_3_CODE"]
        del self.weekly_hxltags["Date_reported"]
        national_headers = base_headers + list(self.weekly_hxltags.keys())
        national_hxltags = base_hxltags + list(self.weekly_hxltags.values())

        super().__init__(
            "who_covid",
            datasetinfo,
            {
                "national": (tuple(national_headers), tuple(national_hxltags)),
                "global": (tuple(base_headers), tuple(base_hxltags)),
                "gho": (tuple(base_headers), tuple(base_hxltags)),
            },
        )
        self.today = today
        self.outputs = outputs
        self.hrp_countries = hrp_countries
        self.gho_countries = gho_countries
        self.iso3_to_region = iso3_to_region
        self.population_lookup = population_lookup

    def get_who_data(self, url):
        df = pd.read_csv(url, keep_default_na=False)
        df.columns = df.columns.str.strip()
        df = df[
            [
                "Date_reported",
                "Country_code",
                "Cumulative_cases",
                "New_cases",
                "New_deaths",
                "Cumulative_deaths",
            ]
        ]
        df.insert(1, "ISO_3_CODE", df["Country_code"].apply(Country.get_iso3_from_iso2))
        df = df.drop(columns=["Country_code"])

        # cumulative
        df_cumulative = df.sort_values(by=["Date_reported"]).drop_duplicates(
            subset="ISO_3_CODE", keep="last"
        )
        df_cumulative = df_cumulative.drop(
            columns=["Date_reported", "New_cases", "New_deaths"]
        )
        df_world = df_cumulative.sum()
        df_cumulative = df_cumulative.loc[df["ISO_3_CODE"].isin(self.gho_countries), :]
        df_gho = df_cumulative.sum()

        df = df.loc[df["ISO_3_CODE"].isin(self.gho_countries), :]

        df_series = df.copy(
            deep=True
        )  # used in series processing, keeps df unchanged for use elsewhere
        df_series["CountryName"] = df_series["ISO_3_CODE"].apply(
            Country.get_country_name_from_iso3
        )  # goes on to be output as covid series tab

        df["Date_reported"] = pd.to_datetime(df["Date_reported"])
        source_date = df["Date_reported"].max()

        # adding global GHO by date
        df_gho_all = df.groupby("Date_reported").sum()
        df_gho_all["ISO_3_CODE"] = "GHO"
        df_gho_all = df_gho_all.reset_index()

        # adding global HRPs by date
        df_hrp_countries_all = df.loc[df["ISO_3_CODE"].isin(self.hrp_countries), :]
        df_hrp_countries_all = df_hrp_countries_all.groupby("Date_reported").sum()
        df_hrp_countries_all["ISO_3_CODE"] = "HRPs"
        df_hrp_countries_all = df_hrp_countries_all.reset_index()

        # adding regional by date
        dict_regions = pd.DataFrame(
            self.iso3_to_region.items(), columns=["ISO3", "Regional_office"]
        )
        df = pd.merge(
            left=df,
            right=dict_regions,
            left_on="ISO_3_CODE",
            right_on="ISO3",
            how="left",
        )
        df = df.drop(labels="ISO3", axis="columns")
        df_regional = (
            df.groupby(["Date_reported", "Regional_office"]).sum().reset_index()
        )
        df_regional = df_regional.rename(columns={"Regional_office": "ISO_3_CODE"})

        df = df.append(df_gho_all)
        df = df.append(df_hrp_countries_all)
        df = df.append(df_regional)

        return source_date, df_world, df_gho, df_series, df

    def run(self) -> None:
        read_hdx_metadata(self.datasetinfo, today=self.today)

        # get WHO data
        source_date, df_world, df_gho, df_series, df_WHO = self.get_who_data(
            self.datasetinfo["url"]
        )
        df_pop = pd.DataFrame.from_records(
            list(self.population_lookup.items()), columns=["Country Code", "population"]
        )

        # output time series
        series_headers = ["Cumulative_cases", "Cumulative_deaths"]
        series_hxltags = ["#affected+infected", "#affected+killed"]
        series_headers_hxltags = {
            "ISO_3_CODE": "#country+code",
            "CountryName": "#country+name",
            "Date_reported": "#date+reported",
        }
        for i, header in enumerate(series_headers):
            series_headers_hxltags[header] = series_hxltags[i]

        # cumulative numbers for national/daily and old covid viz
        series_name = "covid_series"
        df_series = df_series.drop(["New_cases", "New_deaths"], axis=1)

        self.outputs["gsheets"].update_tab(
            series_name, df_series, series_headers_hxltags, 1000
        )  # 1000 rows in gsheets!/;
        self.outputs["excel"].update_tab(series_name, df_series, series_headers_hxltags)
        self.outputs["json"].update_tab(
            "covid_series_flat", df_series, series_headers_hxltags
        )
        json_df = df_series.groupby("CountryName").apply(lambda x: x.to_dict("r"))
        del series_headers_hxltags[
            "CountryName"
        ]  # prevents it from being output as it is already the key
        for rows in json_df:
            countryname = rows[0]["CountryName"]
            self.outputs["json"].add_data_rows_by_key(
                series_name, countryname, rows, series_headers_hxltags
            )

        df_national = df_series.sort_values(by=["Date_reported"]).drop_duplicates(
            subset="ISO_3_CODE", keep="last"
        )

        def format_0dp(x):
            if isinstance(x, str):
                return x
            return f"{x:.0f}"

        national_columns = [
            dict(
                zip(
                    df_national["ISO_3_CODE"],
                    df_national["Cumulative_cases"].map(format_0dp),
                )
            ),
            dict(
                zip(
                    df_national["ISO_3_CODE"],
                    df_national["Cumulative_deaths"].map(format_0dp),
                )
            ),
        ]

        # Viz and daily PDF trend epi weekly (non-rolling) output
        resampled = (
            df_WHO.drop(columns=["Regional_office"])
            .groupby(["ISO_3_CODE"])
            .resample("W", on="Date_reported")
        )
        new_w = resampled.sum()[["New_cases", "New_deaths"]]
        ndays_w = resampled.count()["New_cases"]
        ndays_w = ndays_w.rename("ndays")

        output_df = pd.merge(
            left=new_w, right=ndays_w, left_index=True, right_index=True, how="inner"
        )
        output_df = output_df[output_df["ndays"] == 7]
        output_df = output_df.reset_index()

        df_by_iso3 = output_df.groupby("ISO_3_CODE")
        output_df["weekly_cum_cases"] = df_by_iso3["New_cases"].cumsum()
        output_df["weekly_cum_deaths"] = df_by_iso3["New_deaths"].cumsum()
        output_df["weekly_new_cases_pc_change"] = df_by_iso3["New_cases"].pct_change()
        output_df["weekly_new_deaths_pc_change"] = df_by_iso3["New_deaths"].pct_change()
        # For percent change, if the diff is actually 0, change nan to 0
        output_df["weekly_new_cases_change"] = df_by_iso3["New_cases"].diff()
        output_df.loc[
            (output_df["weekly_new_cases_pc_change"].isna())
            & (output_df["weekly_new_cases_change"] == 0),
            "weekly_new_cases_pc_change",
        ] = 0.0
        output_df["weekly_new_deaths_change"] = df_by_iso3["New_deaths"].diff()
        output_df.loc[
            (output_df["weekly_new_deaths_pc_change"].isna())
            & (output_df["weekly_new_deaths_change"] == 0),
            "weekly_new_deaths_pc_change",
        ] = 0.0

        # Add pop to output df
        output_df = output_df.merge(
            df_pop, left_on="ISO_3_CODE", right_on="Country Code", how="left"
        ).drop(columns=["Country Code"])
        output_df = output_df.rename(
            columns={"New_cases": "weekly_new_cases", "New_deaths": "weekly_new_deaths"}
        )
        # Get cases per hundred thousand
        output_df["weekly_new_cases_per_ht"] = (
            output_df["weekly_new_cases"] / output_df["population"] * 1e5
        )
        output_df["weekly_new_deaths_per_ht"] = (
            output_df["weekly_new_deaths"] / output_df["population"] * 1e5
        )

        output_df["Date_reported"] = output_df["Date_reported"].apply(
            lambda x: x.strftime("%Y-%m-%d")
        )
        output_df = output_df.drop(["ndays"], axis=1)
        trend_name = "covid_trend"
        self.outputs["gsheets"].update_tab(trend_name, output_df, self.trend_hxltags)
        self.outputs["excel"].update_tab(trend_name, output_df, self.trend_hxltags)
        # Save as JSON
        json_df = (
            output_df.replace([numpy.inf, -numpy.inf, numpy.nan], "")
            .groupby("ISO_3_CODE")
            .apply(lambda x: x.to_dict("r"))
        )
        grouped_trend_hxltags = deepcopy(self.trend_hxltags)
        del grouped_trend_hxltags["ISO_3_CODE"]

        def format_0dp(x):
            return number_format(x, "%.0f")

        def format_4dp(x):
            return number_format(x, "%.4f", False)

        for rows in json_df:
            countryiso = rows[0]["ISO_3_CODE"]
            for row in rows:
                for header in grouped_trend_hxltags:
                    if any(header.endswith(x) for x in ("per_ht", "pc_change")):
                        row[header] = format_4dp(row[header])
            self.outputs["json"].add_data_rows_by_key(
                self.name, countryiso, rows, grouped_trend_hxltags
            )

        df_national = output_df.sort_values(by=["Date_reported"]).drop_duplicates(
            subset="ISO_3_CODE", keep="last"
        )

        for header in self.weekly_hxltags:
            if any(header.endswith(x) for x in ("per_ht", "pc_change")):
                fn = format_4dp
            else:
                fn = format_0dp
            national_columns.append(
                dict(zip(df_national["ISO_3_CODE"], df_national[header].map(fn)))
            )
        self.datasetinfo["date"] = source_date
        self.values["national"] = tuple(national_columns)
        global_values = self.get_values("global")
        global_values[0]["global"] = int(df_world["Cumulative_cases"])
        global_values[1]["global"] = int(df_world["Cumulative_deaths"])
        gho_values = self.get_values("gho")
        gho_values[0]["global"] = int(df_gho["Cumulative_cases"])
        gho_values[1]["global"] = int(df_gho["Cumulative_deaths"])
