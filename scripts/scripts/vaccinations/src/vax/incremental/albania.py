import re

from bs4 import BeautifulSoup
import pandas as pd

from vax.utils.utils import get_soup
from vax.utils.incremental import clean_count, merge_with_current_data
from vax.utils.dates import clean_date


class Albania:

    def __init__(self, source_url: str, location: str):
        self.source_url = source_url
        self.location = location
        self._num_max_pages = 3
        self.regex = {
            "title": r"Vaksinimi antiCOVID\/ Kryhen [0-9,]+ vaksinime",
            "date":  r"Postuar më: (\d{1,2}\/\d{1,2}\/202\d)",
            "total_vaccinations": r"total ([\d,]+) doza të vaksinës ndaj COVID19",
            "people_fully_vaccinated": r"Prej tyre,? ([\d,]+) qytetarë i kanë marrë të dyja dozat e vaksinës antiCOVID",
        }

    def read(self, last_update: str) -> pd.DataFrame:
        data = []
        for cnt in range(1, self._num_max_pages+1):
            # print(f"page: {cnt}")
            url = f"{self.source_url}/{cnt}/"
            soup = get_soup(url, verify=False)
            data_, proceed = self.parse_data(soup, last_update)
            data.extend(data_)
            if not proceed:
                break
        return pd.DataFrame(data)

    def parse_data(self, soup: BeautifulSoup, last_update: str) -> tuple:
        elems = self.get_elements(soup)
        records = []
        for elem in elems:
            if elem["date"] > last_update:
                # print(elem["date"], elem)
                soup = get_soup(elem["link"], verify=False)
                record = {
                    "source_url": elem["link"],
                    "date": elem["date"],
                    **self.parse_data_news_page(soup),
                }
                records.append(record)
            else: 
                # print(elem["date"], "END")
                return records, False
        return records, True

    def get_elements(self, soup: BeautifulSoup) -> list:
        news = soup.find(id="leftContent").find_all("h2", text=re.compile(self.regex["title"]))
        news = [
            {"link": self.parse_link(n), "date": self.parse_date(n)} 
        for n in news]
        return news

    def parse_data_news_page(self, soup: BeautifulSoup):
        total_vaccinations = re.search(self.regex["total_vaccinations"], soup.text)
        people_fully_vaccinated = re.search(self.regex["people_fully_vaccinated"], soup.text)
        metrics = {}
        if total_vaccinations:
            metrics["total_vaccinations"] = clean_count(total_vaccinations.group(1))
        if people_fully_vaccinated:
            metrics["people_fully_vaccinated"] = clean_count(people_fully_vaccinated.group(1))
        return metrics

    def parse_date(self, elem):
        match = re.search(self.regex["date"], elem.parent.text)
        return clean_date(match.group(1), "%d/%m/%Y", minus_days=1)

    def parse_link(self, elem):
        return elem.a.get("href")

    def pipe_people_vaccinated(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(people_vaccinated=df.total_vaccinations-df.people_fully_vaccinated)

    def pipe_drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.sort_values("date")
            .drop_duplicates(
                subset=["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"],
                keep="first"
            )
        )

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine="Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac, Sputnik V")

    def pipe_select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[[
            "location",
            "date",
            "vaccine",
            "source_url",
            "total_vaccinations",
            "people_vaccinated",
            "people_fully_vaccinated"
        ]]

    def pipeline(self, df: pd.Series) -> pd.Series:
        return (
            df
            .pipe(self.pipe_people_vaccinated)
            .pipe(self.pipe_drop_duplicates)
            .pipe(self.pipe_location)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_select_output_columns)
            .sort_values(by="date")
        )

    def to_csv(self, paths):
        """Generalized."""
        output_file = paths.tmp_vax_out(self.location)
        last_update = pd.read_csv(output_file).date.max()
        df = self.read(last_update)
        if df is not None:
            df = df.pipe(self.pipeline)
            df = merge_with_current_data(df, output_file)
            df = df.pipe(self.pipe_drop_duplicates)
            df.to_csv(output_file, index=False)


def main(paths):
    Albania(
        source_url="https://shendetesia.gov.al/category/lajme/page",
        location="Albania",
    ).to_csv(paths) 


if __name__ == "__main__":
    main()
