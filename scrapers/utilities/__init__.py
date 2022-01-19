from hdx.utilities.text import get_fraction_str


def calculate_ratios(ratios, items_per_country, affected_items_per_country):
    for countryiso in items_per_country:
        if countryiso in affected_items_per_country:
            ratios[countryiso] = get_fraction_str(
                affected_items_per_country[countryiso], items_per_country[countryiso]
            )
        else:
            ratios[countryiso] = "0.0"
    return ratios
