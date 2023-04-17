import logging

import numpy as np
import pandas as pd
from asreviewcontrib.preprocess import utils
from asreviewcontrib.preprocess.data.load import load_data
from asreviewcontrib.preprocess.deduplication import dd_utils
from asreviewcontrib.preprocess.io import io_utils


def update_records(
    input_path,
    output_path,
    email=None,
    doi_update_method="crossref",
    data_update_method="openalex",
    local_database="tinydb",
):
    """Find missing information and update records

    Parameters
    ----------
    input_path: str
        Path of input dataset with missing metadata
    output_path: str
        Path to save the updated dataset
    email: str
        Email address to get polite access to updater APIs such as Openalex and Crossref
    doi_update_method: str
        DOI Updater, by default "crossref"
    data_update_method: str
        Data Updater, by default "openalex"
    local_database: str
        Local database method for saving retrieved metadata, by default "tinydb"
    """
    records_df, _ = load_data(input_path)

    col_specs = io_utils._get_column_spec(records_df)
    print(f"Column Definitions: {col_specs}")

    db = utils._localdb_class_from_entry_point(local_database)()
    doi_updater = utils._updater_class_from_entry_point(doi_update_method)()
    data_updater = utils._updater_class_from_entry_point(data_update_method)()

    # Get polite access to updater APIs such as Openalex and Crossref
    if email:
        doi_updater._use_email(email)
        data_updater._use_email(email)

    # Clean dois in case not already cleaned as they will be used
    # to retrieve record metadata
    records_df[col_specs["doi"]] = records_df[col_specs["doi"]].apply(
        dd_utils.clean_doi
    )

    # Make missing values as NAN
    string_columns = records_df.select_dtypes("object").columns
    records_df[string_columns] = (
        records_df[string_columns]
        .fillna("")
        .applymap(lambda val: np.nan if len(val) == 0 else val)
    )

    # Find records where title and year are available and doi is missing
    records_df = doi_updater.retrieve_dois(records_df)

    # Make year column string type
    records_df["year"] = records_df["year"].astype("object")

    # Find records where DOI is available and fields required
    # for deduplication are missing
    records_df["missing_data"] = (
        records_df[col_specs["title"]].isna()
        | records_df[col_specs["authors"]].isna()
        | records_df[col_specs["abstract"]].isna()
        | records_df[col_specs["year"]].isna()
        | records_df[col_specs["journal"]].isna()
        | records_df[col_specs["pages"]].isna()
        | records_df[col_specs["volume"]].isna()
        | records_df[col_specs["number"]].isna()
        | records_df[col_specs["isbn"]].isna()
    ) & records_df[col_specs["doi"]].notna()

    n_missing_abstracts_before = _get_no_of_missing_abstracts(records_df, col_specs)

    doi_list = records_df[col_specs["doi"]][records_df["missing_data"]].values
    retrieved_metadata = data_updater.retrieve_metadata(db, doi_list)
    retrieved_records_df = data_updater.parse_metadata(retrieved_metadata)

    records_df_only_doi = pd.DataFrame({"doi": records_df[col_specs["doi"]].values})
    retrieved_records_df = records_df_only_doi.merge(
        retrieved_records_df, on="doi", how="left"
    )  # .set_index("record_id")

    # Update original df only where the data was missing and is retrieved
    updated_records_df = records_df.combine_first(retrieved_records_df)

    n_missing_abstracts_after = _get_no_of_missing_abstracts(
        updated_records_df, col_specs
    )
    print(f"{n_missing_abstracts_before} abstracts were missing.")
    print(
        f"{n_missing_abstracts_before - n_missing_abstracts_after} missing abstracts were retrieved."
    )
    print(f"{n_missing_abstracts_after} abstracts are still missing.\n")

    updated_records_df.to_csv(output_path)
    print(f"Updated dataset saved to {output_path}")
    return updated_records_df


def _get_no_of_missing_abstracts(records_df, col_specs):
    """Gives number of missing abstracts in the dataset"""
    return sum(records_df[col_specs["abstract"]].isna())


def _get_no_of_missing_dois(records_df, col_specs):
    """Gives number of missing DOIs in the dataset"""
    return sum(records_df[col_specs["doi"]].isna())


# TODO: Add other data and doi updaters using free APIs
# TODO: Better logging
