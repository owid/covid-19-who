import pandas as pd
import numpy as np

from vax.utils.incremental import increment
from vax.utils.checks import VACCINES_ONE_DOSE
from vax.utils.who import VACCINES_WHO_MAPPING


# Dict mapping WHO country names -> OWID country names
COUNTRIES = {
    "Afghanistan": "Afghanistan",
    "Angola": "Angola",
    "Benin": "Benin",
    "Bonaire, Sint Eustatius and Saba": "Bonaire Sint Eustatius and Saba",
    "Cabo Verde": "Cape Verde",
    "Cameroon": "Cameroon",
    "Central African Republic": "Central African Republic",
    "Comoros": "Comoros",
    "Congo": "Congo",
    "Democratic Republic of the Congo": "Democratic Republic of Congo",
    "Djibouti": "Djibouti",
    "Egypt": "Egypt",
    "Guinea-Bissau": "Guinea-Bissau",
    "Iran (Islamic Republic of)": "Iran",
    "Jamaica": "Jamaica",
    "Lesotho": "Lesotho",
    "Liberia": "Liberia",
    "Libya": "Libya",
    "Madagascar": "Madagascar",
    "Mauritania": "Mauritania",
    "Mauritius": "Mauritius",
    "Mozambique": "Mozambique",
    "Myanmar": "Myanmar",
    "Nicaragua": "Nicaragua",
    "Niger": "Niger",
    "Papua New Guinea": "Papua New Guinea",
    "Rwanda": "Rwanda",
    "Sao Tome and Principe": "Sao Tome and Principe",
    "Sint Maarten": "Sint Maarten (Dutch part)",
    "Somalia": "Somalia",
    "South Sudan": "South Sudan",
    "Sudan": "Sudan",
    "Tajikistan": "Tajikistan",
    "Timor-Leste": "Timor",
    "Turkmenistan": "Turkmenistan",
    "Yemen": "Yemen",
}


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source)


def source_checks(df: pd.DataFrame) -> pd.DataFrame:
    if len(df) > 300:
        raise ValueError(f"Check source, it may contain updates from several dates! Shape found was {df.shape}")
    if df.groupby("COUNTRY").DATE_UPDATED.nunique().nunique() == 1:
        if df.groupby("COUNTRY").DATE_UPDATED.nunique().unique()[0] != 1:
            raise ValueError("Countries have more than one date update!")
    else:
        raise ValueError("Countries have more than one date update!")
    return df


def filter_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Get rows from selected countries."""
    df = df[df.DATA_SOURCE == "REPORTING"].copy()
    df = df[
        (df.TOTAL_VACCINATIONS >= df.PERSONS_VACCINATED_1PLUS_DOSE) |
        (df.PERSONS_VACCINATED_1PLUS_DOSE.isnull())
    ]
    df["COUNTRY"] = df.COUNTRY.replace(COUNTRIES)
    df = df[df.COUNTRY.isin(COUNTRIES.values())]
    return df


def vaccine_checks(df: pd.DataFrame) -> pd.DataFrame:
    vaccines_used = set(
        df.VACCINES_USED
        .dropna()
        .apply(lambda x: [xx.strip() for xx in x.split(",")])
        .sum()
    )
    vaccines_unknown = vaccines_used.difference(VACCINES_WHO_MAPPING)
    if vaccines_unknown:
        raise ValueError(
            f"Unknown vaccines {vaccines_unknown}. Update `vax.utils.who.VACCINES_WHO_MAPPING` accordingly."
        )
    return df


def map_vaccines_func(row) -> tuple:
    """Replace vaccine names and create column `only_2_doses`."""
    if pd.isna(row.VACCINES_USED):
        raise ValueError("Vaccine field is NaN")
    vaccines = pd.Series(row.VACCINES_USED.split(","))
    vaccines = vaccines.replace(VACCINES_WHO_MAPPING)
    only_2doses = all(-vaccines.isin(pd.Series(VACCINES_ONE_DOSE)))

    return pd.Series([", ".join(sorted(vaccines.unique())), only_2doses])


def map_vaccines(df: pd.DataFrame) -> pd.DataFrame:
    # Based on the list of known vaccines, identifies whether each country is using only 2-dose
    # vaccines or also some 1-dose vaccines. This determines whether people_fully_vaccinated can be
    # calculated as total_vaccinations - people_vaccinated.
    # Vaccines check
    df[["VACCINES_USED", "only_2doses"]] = df.apply(map_vaccines_func, axis=1)
    return df


def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df.only_2doses, "people_fully_vaccinated"] = (
        df.TOTAL_VACCINATIONS - df.PERSONS_VACCINATED_1PLUS_DOSE
    )
    df.loc[~df.only_2doses, "people_fully_vaccinated"] = None
    df[["PERSONS_VACCINATED_1PLUS_DOSE", "people_fully_vaccinated"]] = (
        df[["PERSONS_VACCINATED_1PLUS_DOSE", "people_fully_vaccinated"]]
        .astype("Int64").fillna(pd.NA)
    )
    df.loc[:, "TOTAL_VACCINATIONS"] = df["TOTAL_VACCINATIONS"].fillna(np.nan)
    return df


def increment_countries(df: pd.DataFrame, paths):
    for row in df.iterrows():
        row = row[1]
        print(row["COUNTRY"])        
        cond = row[["PERSONS_VACCINATED_1PLUS_DOSE", "people_fully_vaccinated", "TOTAL_VACCINATIONS"]].isnull().all()
        if not cond:
            increment(
                paths=paths,
                location=row["COUNTRY"],
                total_vaccinations=row["TOTAL_VACCINATIONS"],
                people_vaccinated=row["PERSONS_VACCINATED_1PLUS_DOSE"],
                people_fully_vaccinated=row["people_fully_vaccinated"],
                date=row["DATE_UPDATED"],
                vaccine=row["VACCINES_USED"],
                source_url="https://covid19.who.int/",
            )


def main(paths):
    source_url = "https://covid19.who.int/who-data/vaccination-data.csv"
    df = (
        read(source_url)
        .pipe(source_checks)
        .pipe(filter_countries)
        .pipe(vaccine_checks)
        .pipe(map_vaccines)
        .pipe(calculate_metrics)
    )
    increment_countries(df, paths)


if __name__ == "__main__":
    main()
