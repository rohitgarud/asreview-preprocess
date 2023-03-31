import logging

import numpy as np
from asreviewcontrib.preprocess import utils
from asreviewcontrib.preprocess.deduplication import dd_utils
from asreviewcontrib.preprocess.io import io_utils


def update_records(
    records_df,
    doi_update_method="crossref",
    data_update_method="openalex",
    local_database="tinydb",
):
    """Find missing information and update records

    Parameters
    ----------
    records_df : pandas dataframe
        Dataframe of citation records from imported dataset
    data_update_method: str
        Data Updater, by default "openalex"
    local_database:
        Local database for retrieving and saving matadata, by default "tinydb"
    """

    col_specs = io_utils._get_column_spec(records_df)

    db = utils._localdb_class_from_entry_point(local_database)
    doi_updater = utils._updater_class_from_entry_point(doi_update_method)
    data_updater = utils._updater_class_from_entry_point(data_update_method)

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

    # Find records where title and year are available and doi is missing
    records_df["missing_doi"] = (
        records_df[col_specs["title"]].notna() & records_df[col_specs["year"]].notna()
    ) & records_df[col_specs["doi"]].isna()

    n_missing_dois_before = _get_no_of_missing_dois(records_df, col_specs)

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
        f"{n_missing_abstracts_before} abstracts were missing.\n"
        f"{n_missing_abstracts_before - n_missing_abstracts_after} missing abstracts were retrieved.\n"
        f"{n_missing_abstracts_after} abstracts are still missing.\n"
    )

    return updated_records_df


def _get_no_of_missing_abstracts(records_df, col_specs):
    """Gives number of missing abstracts in the dataset"""
    return sum(records_df[col_specs["abstract"]].isna())


def _get_no_of_missing_dois(records_df, col_specs):
    """Gives number of missing DOIs in the dataset"""
    return sum(records_df[col_specs["doi"]].isna())
