import logging
import re
import os
from datetime import datetime
import csv

import pandas as pd
import numpy as np
from unidecode import unidecode

from asreview.config import COLUMN_DEFINITIONS
from asreview.exceptions import BadFileFormatError
from asreview.io.utils import _standardize_dataframe, type_from_column
from asreview.data import load_data as asreview_load_data
from xml_reader import EndnoteXMLReader

# Additional column definitions required for deduplication
DEDUPLICATION_COLUMN_DEFINITIONS = {
    "record_id": ["record_id", "rec-number"],
    "year": ["year"],
    "ref_type": ["ref_type", "type_of_reference"],
    "journal": ["journal"],
    "volume": ["volume"],
    "pages": ["pages", "start_page"],
    "number": ["number", "issue"],
    "isbn": ["isbn", "issn"],
    "secondary_title": ["secondary_title", "secondary title", "secondary-title"],
}

# Dictionary of journal name abbreviations and full forms
with open("all_journal_abbreviations.csv", "r") as f:
    reader = csv.reader(f)
    all_journal_abbreviations = {}

    for row in reader:
        all_journal_abbreviations[row[0]] = row[1].encode("ascii", "ignore").decode()


# Load Data
def load_data(input_filepath):
    """Load data from file, URL or plugin.

    Parameters
    ----------
    input_filepath : str, pathlib.Path
        File path, URL, or alias of extension dataset.
        Supported file extensions are .csv, .tab, .tsv, .xlsx, .ris, .txt and .xml (Endnote XML)
    """
    try:
        df = asreview_load_data(input_filepath).df
    except BadFileFormatError:
        try:
            df = asreview_load_data(input_filepath, reader=EndnoteXMLReader).df
        except:
            raise ValueError("This file format is not supported yet.")

    df, col_specs = _standardize_dataframe_for_deduplication(
        df, column_spec=DEDUPLICATION_COLUMN_DEFINITIONS
    )
    return df, col_specs


# Clean different fields to unify them to a common format
def clean_doi(doi):
    """unify DOIs to a common format https://doi.org/{doi}"""
    if len(doi) > 0:
        doi = re.findall(r"(10\..+)", doi)
        if len(doi) > 0:
            return f"https://doi.org/{doi[0].strip()}"
    return doi


def clean_pages(pages):
    """Unify page numbers to a common format. Changes formats like 311-7 to 311-317."""
    try:
        # Checking if date is missfilled as pages in input dataset
        is_date = re.findall(r"\d{2}-\d{2}-\d{4}", pages)[0]
    except:
        if pages:
            pages = re.sub(r"[^0-9-]", "", pages)
            start, end = re.findall(r"(?P<start>[0-9]*)-?(?P<end>[0-9]*)", pages)[0]
            if end:
                len_diff = len(start) - len(end)
                if len_diff > 0:
                    end = f"{start[:len_diff]}{end}"
                return f"{start}-{end}"
            return start
    return ""


def clean_journal(journal):
    """Expand abbreviated journal names"""
    preprocess_journal = journal.encode("ascii", "ignore").decode("ascii")
    preprocess_journal = re.sub("[^a-zA-Z0-9\s]", "", preprocess_journal)
    # TODO: Handle Accents better

    try:
        return all_journal_abbreviations[preprocess_journal]
    except KeyError:
        return journal


def clean_authors(authors):
    """Unify author names to a common format."""
    authors = unidecode(authors)
    matches = re.finditer(
        r"\s?(?P<last>([A-Z]?[a-z]*\s?)*-?[A-Z][a-z]+),?\s+(?P<first>[A-Z])(?P<full>[a-z])?.?\s?(?P<middle>[A-Z])?.?",
        authors.replace('"', ""),
    )
    authors = " ".join(
        f"{match.group('last')} {match.group('first')} {match.group('middle')}"
        if match.group("middle")
        else f"{match.group('last')} {match.group('first')}"
        for match in matches
    )
    return authors


def clean_isbn(isbn):
    """Unify ISBNs/ISSNs to a common format."""
    return isbn


def get_output_path(args):
    """Get output path based on user input.

    If path is given, check if it is accepted format. If path is not given, output filename is same as input filename with datetime added"""

    input_path = args.input_path[0]
    if args.output_path:
        output_path = args.output_path
        if not output_path.endswith(".csv"):
            if "." in output_path:
                raise ValueError(
                    "Output File extensions other than .csv are not supported yet"
                )
            else:
                output_path += ".csv"
    else:
        output_path = os.path.basename(input_path)
        output_path = f"{os.path.splitext(output_path)[0]}-deduplicated-{datetime.now().strftime('%Y%m%dT%H%M')}.csv"
    return output_path


def _standardize_dataframe_for_deduplication(df, column_spec={}):
    """Standardize the dataset first for ASreview import and then for deduplication and add missing columns if required
    Arguments
    ---------
    df: pandas.DataFrame
        Unclean dataframe to be cleaned up.
    Returns
    -------
    pd.DataFrame:
        Cleaned dataframe with proper column names.
    """
    df, all_column_spec = _standardize_dataframe(df)

    # map columns on column specification
    col_names = list(df.columns)
    for column_name in col_names:
        # First try the supplied column specifications if supplied.
        data_type = type_from_column(column_name, column_spec)
        if data_type is not None:
            all_column_spec[data_type] = column_name
            continue
        # Then try the standard specifications in ASReview.
        data_type = type_from_column(column_name, COLUMN_DEFINITIONS)
        if data_type is not None:
            all_column_spec[data_type] = column_name

    col_names = list(all_column_spec)
    cols_for_dedupe = [
        "authors",
        "title",
        "abstract",
        "year",
        "journal",
        "doi",
        "volume",
        "pages",
        "number",
        "isbn",
    ]

    # Check if we have all the required columns and add empty columns if missing.
    for col in cols_for_dedupe:
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

    # print(f"col_names: {col_names}")
    # print(f"col_specs: {all_column_spec}")

    # Use secondary title as journal if journal name is missing
    if "secondary_title" in col_names:
        logging.warning(
            f"Secondary title column will be used for filling missing values in Journal column"
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
