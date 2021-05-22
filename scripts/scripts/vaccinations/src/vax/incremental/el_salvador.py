from datetime import datetime
import json

import pandas as pd
from bs4 import BeautifulSoup

from vax.utils.incremental import enrich_data, increment, clean_count
from vax.utils.utils import get_soup
from vax.utils.dates import clean_date


def read(source: str) -> pd.Series:
    soup = get_soup(source)
    link = parse_infogram_link(soup)
    soup = get_soup(link)
    infogram_data = parse_infogram_data(soup)
    return pd.Series({
        "date": parse_infogram_date(infogram_data),
        "source_url": source,
        **parse_infogram_vaccinations(infogram_data)
    })


def parse_infogram_link(soup: BeautifulSoup) -> str:
    url_end = soup.find(class_="infogram-embed").get("data-id")
    return f"https://e.infogram.com/{url_end}"


def parse_infogram_data(soup: BeautifulSoup) -> dict:
    for script in soup.find_all("script"):
        if "infographicData" in str(script):
            json_data = (
                script.string[:-1]
                .replace("window.infographicData=", "")
            )
            json_data = json.loads(json_data)
            break
    json_data = json_data["elements"]["content"]["content"]["entities"]
    return json_data


def _get_infogram_value(infogram_data: dict, field_id: str, join_text: bool = False):
    if join_text:
        return "".join(x["text"] for x in infogram_data[field_id]["props"]["content"]["blocks"])
    return infogram_data[field_id]["props"]["content"]["blocks"][0]["text"]


def parse_infogram_vaccinations(infogram_data: dict) -> int:
    total_vaccinations = clean_count(_get_infogram_value(infogram_data, "4f66ed81-151f-4b97-aa3c-4927bde058b2"))
    people_vaccinated = clean_count(_get_infogram_value(infogram_data, "4048eac1-24ba-4e24-b081-61dfa0281a0e"))
    people_fully_vaccinated = clean_count(_get_infogram_value(infogram_data, "50a2486f-7dca-4afd-a551-bd24665d7314"))
    return {
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated
    }


def parse_infogram_date(infogram_data: dict) -> str:
    x = _get_infogram_value(infogram_data, "525b6366-cc8a-4646-b67a-5c9bfca66e22", join_text=True)
    dt = clean_date(x, "RESUMEN DE VACUNACIÓN DIARIA %d-%b-%y", "es")
    return dt


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "El Salvador")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Sinovac")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
    )


def main(paths):
    source = "https://covid19.gob.sv/"
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
