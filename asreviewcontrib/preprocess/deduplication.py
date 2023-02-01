import numpy as np
import pandas as pd
from pandas.api.types import is_string_dtype

from asreview.data import load_data
from asreview.config import COLUMN_DEFINITIONS
from asreview.config import LABEL_NA
from asreview.datasets import DatasetManager
from asreview.datasets import DatasetNotFoundError
from asreview.exceptions import BadFileFormatError
from asreview.io import PaperRecord
from asreview.io.utils import convert_keywords
from asreview.io.utils import type_from_column
from asreview.utils import get_entry_points
from asreview.utils import is_iterable
from asreview.utils import is_url


def deduplication(input_path, output_path, methods, pid, drop_duplicates):
    dataset = load_data(input_path)

    for method in methods:
        if method == "title":
            pass
        if method == "abstract":
            pass
        if method == "pid":
            if pid not in dataset.columns:
                print(
                    f"Not using {pid} for deduplication"
                    "because there is no such data available in the dataset."
                )
                continue
            else:
                pass
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


# def drop_duplicates(self, pid="doi", inplace=False, reset_index=True):

#     df = self.df[~self.duplicated(pid)]

#     if reset_index:
#         df = df.reset_index(drop=True)
#     if inplace:
#         self.df = df
#         return
#     return df
