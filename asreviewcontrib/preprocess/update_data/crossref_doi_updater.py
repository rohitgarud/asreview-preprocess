import datetime
import string
import urllib

import numpy as np
import pandas as pd
import requests
from asreviewcontrib.preprocess.io import io_utils
from asreviewcontrib.preprocess.update_data.base import BaseDOIUpdater
from tqdm import tqdm


class CrossrefDOIUpdater(BaseDOIUpdater):
    def __init__(self):
        super(CrossrefDOIUpdater, self).__init__()
        self.email = None

    def _use_email(self, email):
        self.email = email
        # TODO: Make use of email in crossref API call

    def retrieve_dois(self, records_df: pd.DataFrame) -> pd.DataFrame:
        """Requests Crossref with title-year combination and returns DOI if
        good enough match is found.

        Parameters
        ----------
        records_df : pd.DataFrame
            Dataset with missing DOIs

        Returns
        -------
        pd.DataFrame
            Dataset with filled missing DOIs if found
        """
        col_specs = io_utils._get_column_spec(records_df)
        data_df = records_df.copy()

        # Set invalid years to None
        data_df.loc[:, col_specs["year"]].loc[
            np.where(
                (data_df[col_specs["year"]] <= 1800)
                | (data_df[col_specs["year"]] >= datetime.date.today().year + 2)
            )
        ] = None

        # Make year column string type
        data_df["year"] = data_df["year"].astype("object")

        # Make missing title and year values as NAN
        cols = [col_specs["title"], col_specs["year"]]
        data_df[cols] = (
            data_df[cols].fillna("").applymap(lambda val: np.nan if not val else val)
        )

        # Check if DOI is missing
        # TODO: Check if name and year is available in localdb
        missing_doi_count = data_df[col_specs["doi"]].isna().sum()
        print(f"Requesting Crossref to infer {missing_doi_count} missing DOIs")

        data_df[col_specs["title"]] = data_df[col_specs["title"]].apply(
            lambda url: urllib.parse.quote(url) if not pd.isna(url) else np.nan
        )
        # TODO: Remove limit after testing
        counter = 0
        for i, row in tqdm(
            data_df[data_df[col_specs["doi"]].isna()].iterrows(),
            desc="Finding missing DOIs",
        ):
            counter += 1
            data_df.loc[i, col_specs["doi"]] = self._crossref_doi_finder(row)
            if counter > 5:
                break

        fixed_doi_count = missing_doi_count - data_df[col_specs["doi"]].isna().sum()
        print(
            f"Out of {missing_doi_count} initially missing DOIs, {fixed_doi_count} ({100 * fixed_doi_count / missing_doi_count:.2f}%) are found"
        )

        data_df.loc[~data_df.doi.isna(), col_specs["doi"]] = data_df[
            ~data_df.doi.isna()
        ].doi.apply(urllib.parse.unquote)

        records_df[col_specs["doi"]] = data_df[col_specs["doi"]]
        return records_df

    @staticmethod
    def _crossref_doi_finder(row):
        headers = {"Accept": "application/json"}

        try:
            title = urllib.parse.quote(row.title)
        except Exception:
            print("Failed to encode title: ", row.title)
            return None
        year = str(int(row.year))

        url = f"https://api.crossref.org/works/?query.title={title}&filter=from-pub-date:{year},until-pub-date:{year}"
        r = requests.get(url, headers=headers)

        try:
            first_entry = r.json()["message"]["items"][0]
            title, found_title = [
                s.translate(string.punctuation).lower()
                for s in [row.title, first_entry["title"][0]]
            ]
            perfect_match = (title in found_title) or (found_title in title)
            if perfect_match:
                return first_entry["DOI"].lower()
        except Exception:
            # JSON decoding error
            print("JSON failed to decode response for: " + row.title)
            return None

        return None
