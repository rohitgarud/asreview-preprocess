import logging

import numpy as np
from asreviewcontrib.preprocess.deduplication import dd_utils
from asreviewcontrib.preprocess.utils import (
    _localdb_class_from_entry_point,
    _updater_class_from_entry_point,
)


def update_records(
    records_df, col_specs, update_method="openalex", local_database="tinydb"
):
    """Find missing information and update records

    Parameters
    ----------
    records_df : pandas dataframe
        Dataframe of citation records from imported dataset
    col_specs : dict
        Column definitions for mapping column names to standardized names
    update_method: str
        Updater, by default "openalex"
    local_database:
        Local database for retrieving and saving matadata, by default "tiny"
    """

    db = _localdb_class_from_entry_point(local_database)
    updater = _updater_class_from_entry_point(update_method)

    # Clean dois in case not already cleaned as they will be used
    # to retrieve record metadata
    records_df[col_specs["doi"]] = records_df[col_specs["doi"]].apply(
        dd_utils.clean_doi
    )

    # Make year column string type
    records_df["year"] = records_df["year"].astype("object")

    # Make missing values as NAN
    string_columns = records_df.select_dtypes("object").columns
    records_df[string_columns] = (
        records_df[string_columns]
        .fillna("")
        .applymap(lambda val: np.nan if len(val) == 0 else val)
    )

    # Find records where DOI is available and fields required
    # for deduplication are missing
    records_df["missing_data"] = (
        records_df[col_specs["title"]].isna()
        | records_df[col_specs["authors"]].isna()
        | records_df[col_specs["abstract"]].isna()
        | records_df[col_specs["year"]].isna()
        | records_df[col_specs["journal"]].isna()
        | records_df[col_specs["doi"]].isna()
        | records_df[col_specs["pages"]].isna()
        | records_df[col_specs["volume"]].isna()
        | records_df[col_specs["number"]].isna()
        | records_df[col_specs["isbn"]].isna()
    ) & records_df[col_specs["doi"]].notna()

    n_missing_abstracts_before = _get_no_of_missing_abstracts(records_df, col_specs)
    logging.info(f"{n_missing_abstracts_before} abstracts were missing.")

    doi_list = records_df[col_specs["doi"]][records_df["missing_data"]].values
    retrieved_metadata = updater.retrieve_metadata(db, doi_list)
    retrieved_records_df = updater.parse_metadata(retrieved_metadata)

    retrieved_records_df = (
        records_df[col_specs["doi"]]
        .reset_index()
        .merge(retrieved_records_df, on="doi")
        .set_index("record_id")
    )

    # Update original df only where the data was missing and is retrieved
    updated_records_df = records_df.combine_first(retrieved_records_df)

    n_missing_abstracts_after = _get_no_of_missing_abstracts(
        updated_records_df, col_specs
    )
    logging.info(
        f"{n_missing_abstracts_before - n_missing_abstracts_after} missing abstracts were retrieved.\n"
    )
    logging.info(f"{n_missing_abstracts_after} abstracts are still missing.")

    return updated_records_df


def _get_no_of_missing_abstracts(records_df, col_specs):
    """Gives number of missing abstracts in the dataset"""
    return sum(records_df[col_specs["abstract"]].isna())
