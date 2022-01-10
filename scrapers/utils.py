from hdx.location.country import Country


def add_to_results(headers, values, datasetinfo, results):
    for level in headers:
        results[level]["headers"] = headers[level]
        results[level]["values"] = values[level]
        results[level]["sources"] = [
            (
                x,
                datasetinfo["date"],
                datasetinfo["source"],
                datasetinfo["source_url"],
            ) for x in headers[level][1]
        ]


def extend_headers(headers, *args):
    result = [list(), list()]
    for i, header in enumerate(headers[:2]):
        for arg in args:
            if arg:
                result[i].extend(arg[i])
                header.extend(arg[i])
    return result


def extend_columns(level, rows, adms, hrp_countries, region, adminone, headers, *args):
    columns = list()
    for arg in args:
        if arg:
            columns.extend(arg)
    if adms is None:
        adms = ["global"]
    for i, adm in enumerate(adms):
        if level == "global":
            row = list()
        elif level == "regional":
            row = [adm]
        elif level == "national":
            ishrp = "Y" if adm in hrp_countries else "N"
            regions = sorted(list(region.iso3_to_region_and_hrp[adm]))
            regions.remove("GHO")
            row = [
                adm,
                Country.get_country_name_from_iso3(adm),
                ishrp,
                "|".join(regions),
            ]
        elif level == "subnational":
            countryiso3 = adminone.pcode_to_iso3[adm]
            countryname = Country.get_country_name_from_iso3(countryiso3)
            adm1_name = adminone.pcode_to_name[adm]
            row = [countryiso3, countryname, adm, adm1_name]
        else:
            raise ValueError("Invalid level")
        append = True
        for existing_row in rows[2:]:
            match = True
            for i, col in enumerate(row):
                if existing_row[i] != col:
                    match = False
                    break
            if match:
                append = False
                row = existing_row
                break
        if append:
            for i, hxltag in enumerate(rows[1][len(row) :]):
                if hxltag not in headers[1]:
                    row.append(None)
        for column in columns:
            row.append(column.get(adm))
        if append:
            rows.append(row)
    return columns


def extend_sources(sources, *args):
    for arg in args:
        if arg:
            sources.extend(arg)
