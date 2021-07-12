import re

import pandas as pd
import tabula

from vax.utils.utils import get_soup
from vax.utils.incremental import clean_count, enrich_data, increment, clean_date


vaccines_mapping = {
    "AstraZeneca": "Oxford/AstraZeneca",
    "Moderna": "Moderna",
}


class Taiwan:

    def __init__(self):
        self.source_url = "https://www.cdc.gov.tw"
        self.location = "Taiwan"

    @property
    def source_data_url(self):
        return f"{self.source_url}/Category/Page/9jFXNbCe-sFK9EImRRi2Og"

    def read(self) -> pd.Series:
        soup = get_soup(self.source_data_url)
        url_pdf = self._parse_pdf_link(soup)
        dfs = self._parse_tables(url_pdf)
        data = self.parse_data(dfs, soup)
        return data

    def parse_data(self, dfs: list, soup):
        stats = self._parse_stats(dfs[0])
        data = pd.Series({
            "total_vaccinations": stats['total_vaccinations'],
            "people_vaccinated": stats['people_vaccinated'],
            "date": self._parse_date(soup),
            "vaccine": self._parse_vaccines(dfs[0]),
        })
        return data

    def _parse_pdf_link(self, soup) -> str:
        a = soup.find(class_="download").find("a")
        url_pdf = f"{self.source_url}{a['href']}"
        for i in range(10):
            soup = get_soup(url_pdf)
            a = soup.find(class_="viewer-button")
            if a is not None:
                break
        return f"{self.source_url}{a['href']}"

    def _parse_tables(self, url_pdf: str) -> int:
        kwargs = {"pandas_options": {"dtype": str, "header": None}}
        dfs = tabula.read_pdf(url_pdf, pages="all", **kwargs)
        return dfs

    def _parse_stats(self, df: pd.DataFrame) -> int:
        if df.shape[1] != 4 or df.iloc[0,0] != "廠牌" or df.iloc[0,1] != "劑次" or not (df.iloc[-1,0] == "總計" or df.iloc[-2,0] == "總計"):
            raise ValueError(f"Table 1: format has changed!")

        num_dose1 = df[df[1] == "第 1劑"].tail(1).values[0][-1]
        num_dose1 = clean_count(num_dose1)

        num_dose2 = df[df[1] == "第 2劑"].tail(1).values[0][-1]
        num_dose2 = clean_count(num_dose2)

        return {
            "total_vaccinations": (num_dose1 + num_dose2),
            "people_vaccinated": num_dose1,
        }

    def _parse_vaccines(self, df: pd.DataFrame) -> str:
        vaccines = set(df.iloc[1:-1, 0].dropna()) - set(['總計'])
        vaccines_wrong = vaccines.difference(vaccines_mapping)
        if vaccines_wrong:
            raise ValueError(f"Invalid vaccines: {vaccines_wrong}")
        return ", ".join(sorted(vaccines_mapping[vax] for vax in vaccines))

    def _parse_date(self, soup) -> str:
        date_raw = soup.find(class_="download").text
        regex = r"(\d{4})\sCOVID-19疫苗(接種)?(統計資料|日報表)"
        date_str = re.search(regex, date_raw).group(1)
        date_str = clean_date("2021" + date_str, "%Y%m%d")
        return date_str

    def pipe_metrics(self, ds: pd.Series) -> pd.Series:
        if "people_vaccinated" in ds:
            return enrich_data(ds, "people_fully_vaccinated", ds.total_vaccinations - ds.people_vaccinated)
        return ds

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_data_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_location)
            .pipe(self.pipe_source)
        )


    def to_csv(self, paths):
        data = self.read().pipe(self.pipeline)
        if "people_vaccinated" in data:
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
        else:
            increment(
                paths=paths,
                location=data["location"],
                total_vaccinations=data["total_vaccinations"],
                date=data["date"],
                source_url=data["source_url"],
                vaccine=data["vaccine"]
            )

def main(paths):
    Taiwan().to_csv(paths)


if __name__ == "__main__":
    main()
