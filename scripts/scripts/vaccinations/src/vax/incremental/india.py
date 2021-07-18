import requests
import pandas as pd

from vax.utils.incremental import enrich_data, increment
from vax.utils.dates import localdatenow, clean_date


class India:
    def __init__(self) -> None:
        self.location = "India"
        self.source_url = "https://api.covid19india.org/data.json"
        # alt: f"https://www.mygov.in/sites/default/files/covid/vaccine/vaccine_counts_today.json"
        # alt: f"https://api.cowin.gov.in/api/v1/reports/v2/getPublicReports?state_id=&district_id=&date={date_str}"
        self.source_url_ref = "https://covid19india.org"

    def read(self) -> pd.Series:
        data = requests.get(self.source_url).json()

        people_vaccinated = data["firstdoseadministered"]
        people_fully_vaccinated = data["seconddoseadministered"]
        total_vaccinations = data["totaldosesadministered"]
        date = data["testedasof"]

        return pd.Series(
            {
                "date": date,
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
                "total_vaccinations": total_vaccinations,
            }
        )

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Covaxin, Oxford/AstraZeneca, Sputnik V")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url_ref)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)
        )

    def export(self, paths):
        data = self.read().pipe(self.pipeline)
        increment(
            paths=paths,
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main(paths):
    India().export(paths)
