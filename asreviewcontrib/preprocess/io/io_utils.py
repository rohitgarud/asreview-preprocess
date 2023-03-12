import logging

import numpy as np
import pandas as pd
from asreview.config import COLUMN_DEFINITIONS
from asreview.io.utils import _standardize_dataframe, type_from_column
from asreviewcontrib.preprocess.config import (
    COLS_FOR_DEDUPE,
    DEDUPLICATION_COLUMN_DEFINITIONS,
)


def _standardize_dataframe_for_deduplication(df, column_spec={}):
    """Standardize the dataset for preprocessing and for ASreview import
    and add missing columns if required

    Arguments
    ---------
    df: pandas.DataFrame
        Unclean dataframe to be cleaned up.
    Returns
    -------
    pd.DataFrame:
        Cleaned dataframe with proper column names.
    """
    df, all_column_spec = _standardize_dataframe(df, column_spec)

    col_names = list(all_column_spec)

    # Check if we have all the required columns and add empty columns if missing.
    for col in COLS_FOR_DEDUPE:
        if col not in col_names:
            all_column_spec[col] = column_spec[col]
            df.insert(5, col, "")
            logging.warning(
                f"Unable to detect '{col}' in the dataset. An emplty column for '{col}' will be added and used for deduplication."
            )

        # Replace NA values with empty strings
        if col != "year":
            try:
                df[all_column_spec[col]] = np.where(
                    pd.isnull(df[all_column_spec[col]]),
                    "",
                    df[all_column_spec[col]].astype(str),
                )
            except KeyError:
                pass

    # Use secondary title as journal if journal name is missing
    if "secondary_title" in col_names:
        logging.warning(
            "Secondary title column will be used for filling missing values in Journal column"
        )
        # Replace NA values with empty strings
        df[all_column_spec["secondary_title"]] = np.where(
            pd.isnull(df[all_column_spec["secondary_title"]]),
            "",
            df[all_column_spec["secondary_title"]].astype(str),
        )

        df["journal"] = np.where(
            df["journal"] == "",
            df["secondary_title"],
            df["journal"],
        )

    # Format missing author names
    df[all_column_spec["authors"]] = np.where(
        (df[all_column_spec["authors"]] == "")
        | (df[all_column_spec["authors"]].str.lower() == "anonymous"),
        "Unknown",
        df[all_column_spec["authors"]],
    )

    return df, all_column_spec


def _get_column_spec(df):
    all_column_spec = {}

    # map columns on column specification
    col_names = list(df)
    for column_name in col_names:

        data_type = type_from_column(column_name, DEDUPLICATION_COLUMN_DEFINITIONS)
        if data_type is not None:
            all_column_spec[data_type] = column_name
            continue

        data_type = type_from_column(column_name, COLUMN_DEFINITIONS)
        if data_type is not None:
            all_column_spec[data_type] = column_name

    return all_column_spec
