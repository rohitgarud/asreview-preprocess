import numpy as np
import pandas as pd
from pandas.api.types import is_string_dtype

from asreview.data import load_data
from asreviewcontrib.preprocess.preprocess_utils import clean_doi


def deduplication(input_path, output_path, methods, pid, drop_duplicates):
    dataset = load_data(input_path)

    for method in methods:
        if method == "title":
            s_title = dataset.title.copy()
            s_title = (
                s_title.str.replace("[^A-Za-z0-9]", "", regex=True)
                .str.lower()
                .str.strip()
                .replace("", None)
            )
            # save boolean series for duplicates based on titles/abstracts
            s_dups_title = (s_title.duplicated()) & (s_title.notnull())

        if method == "abstract":
            s_abstract = dataset.title.copy()
            s_abstract = (
                s_abstract.str.replace("[^A-Za-z0-9]", "", regex=True)
                .str.lower()
                .str.strip()
                .replace("", None)
            )
            # save boolean series for duplicates based on titles/abstracts
            s_dups_title = (s_abstract.duplicated()) & (s_abstract.notnull())
        if method == "pid":
            if pid not in dataset.columns:
                s_dups_pid = None
                print(
                    f"Not using {pid} for deduplication"
                    "because there is no such data available in the dataset."
                )
                continue
            else:
                if is_string_dtype(dataset[pid]):
                    s_pid = dataset[pid].str.strip().replace("", None)
                    if pid == "doi":
                        s_pid = s_pid.apply(clean_doi)

                else:
                    s_pid = dataset[pid]
                # save boolean series for duplicates based on persistent identifiers
                s_dups_pid = (s_pid.duplicated()) & (s_pid.notnull())

        if method == "fuzzy":
            pass

    if drop_duplicates:
        pass

    dataset.to_csv(output_path, index=False)


def duplicated(self, pid="doi"):

    if pid in self.df.columns:
        # in case of strings, strip whitespaces and replace empty strings with None
        if is_string_dtype(self.df[pid]):
            s_pid = self.df[pid].str.strip().replace("", None)
        else:
            s_pid = self.df[pid]

        # save boolean series for duplicates based on persistent identifiers
        s_dups_pid = (s_pid.duplicated()) & (s_pid.notnull())
    else:
        s_dups_pid = None

    # get the texts, clean them and replace empty strings with None
    s = (
        pd.Series(self.texts)
        .str.replace("[^A-Za-z0-9]", "", regex=True)
        .str.lower()
        .str.strip()
        .replace("", None)
    )

    # save boolean series for duplicates based on titles/abstracts
    s_dups_text = (s.duplicated()) & (s.notnull())

    # final boolean series for all duplicates
    if s_dups_pid is not None:
        s_dups = s_dups_pid | s_dups_text
    else:
        s_dups = s_dups_text

    return s_dups
