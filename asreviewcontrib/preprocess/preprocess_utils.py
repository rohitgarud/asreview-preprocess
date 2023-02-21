import logging
import re
import os
from datetime import datetime
import csv

import pandas as pd
import numpy as np
from unidecode import unidecode
from pyalex import Works
from tinydb import TinyDB, Query

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

HTML_ENTITIES = [
    "</\w+>",
    "<\w+>",
    "&nbsp;",
    "&lt;",
    "&gt;",
    "&amp;",
    "&quot;",
    "&apos;",
    "&cent;",
    "&pound;",
    "&yen;",
    "&euro;",
    "&copy;",
    "&reg;",
]

OPENALEX_QUERY_LIMIT = 25

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
def clean_title(title):
    for entity in HTML_ENTITIES:
        title = re.sub(f"{entity}", " ", title)
    title = unidecode(title)
    title = re.sub(r"[^A-Za-z0-9]", " ", title)
    title = re.sub(r" +", " ", title).strip().upper()
    return title


def clean_abstract(abstract):
    for entity in HTML_ENTITIES:
        abstract = re.sub(f"{entity}", " ", abstract)
    abstract = unidecode(abstract)
    abstract = re.sub(r"[^A-Za-z0-9]", " ", abstract)
    abstract = re.sub(r" +", " ", abstract).strip().upper()
    # Remove copywrite information
    # abstract = re.sub(r"COPYRIGHT.*", " ", abstract)
    return abstract


def clean_doi(doi):
    """unify DOIs to a common format https://doi.org/{doi}"""
    if len(doi) > 0:
        doi = re.findall(r"(10\..+)", doi)
        if len(doi) > 0:
            return f"https://doi.org/{doi[0].strip().upper()}"
    return doi


def clean_pages(pages):
    """Unify page numbers to a common format. Changes formats like 311-7 to 311-317."""
    try:
        # Checking if date is missfilled as pages in input dataset
        is_date = re.findall(r"\d{2}-\d{2}-\d{4}", pages)[0]
        return ""
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


def clean_volume(volume):
    """Unify volume to a common format."""
    volume = volume.lower().replace("(no pagination)", "")
    return volume


def clean_isbn(isbn):
    """Unify ISBNs/ISSNs to a common format."""
    isbn = re.sub(r"\s\((Print|Electronic)\).*", "", isbn)
    isbn = re.sub(r"\r", "; ", isbn)
    return isbn


def get_first_author(authors):
    first_author = []
    for item in authors.split():
        if len(item) == 1:
            break
        first_author.append(item)
    first_author = "".join(first_author)
    first_author = re.sub(r"[^A-Za-z]", "", first_author)
    return first_author.upper()


def get_short_title(title):
    short_title = []
    for word in title.split():
        if len(word) > 2:
            short_title.append(word)
        if len(short_title) == 3:
            break
    short_title = "".join(short_title)
    short_title = re.sub(r"[^A-Za-z]", "", short_title)
    return short_title.upper()


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


def retrieve_records_from_localdb(doi_list):
    doi_list_to_retrieve = []
    locally_retrieved_records = {}

    db = TinyDB("./records.json")
    Record = Query()
    for doi in doi_list:
        try:
            metadata = db.search(Record.doi == doi)[0]
            locally_retrieved_records[doi] = metadata
        except IndexError:
            doi_list_to_retrieve.append(doi)

    return locally_retrieved_records, doi_list_to_retrieve


def update_localdb(retrieved_records, doi_list):
    db = TinyDB("./records.json")

    for doi in doi_list:
        try:
            db.insert(retrieved_records[doi])
        except KeyError:
            pass


def retrieve_records(doi_list):
    # Try to retrieve records from local database if available
    retrieved_records, doi_list = retrieve_records_from_localdb(doi_list)

    doi_chunks = (
        doi_list[i : i + OPENALEX_QUERY_LIMIT]
        for i in range(0, len(doi_list), OPENALEX_QUERY_LIMIT)
    )
    # Retrieve records in chunks using OpenAlex "OR" syntax with pyalex
    # TODO: Check if Lens.org api or database can be used freely

    for chunk in doi_chunks:
        data_chunk = Works().filter(doi="|".join(chunk)).get()
        for data in data_chunk:
            retrieved_records[data["doi"]] = data

    update_localdb(retrieved_records, doi_list)

    return retrieved_records


def parse_metadata(retrieved_records):
    # Parse metadata in JSON format and create pandas dataframe with required columns
    parsed_data = []
    for doi, record in retrieved_records.items():
        title = record.get("title")
        authors = ", ".join(
            author["author"]["display_name"] for author in record.get("authorships", [])
        )
        year = record.get("publication_year")
        abstract = record.get("abstract")
        try:
            if record["biblio"].get("first_page"):
                pages = (
                    f"{record['biblio']['first_page']}-{record['biblio']['last_page']}"
                )
            else:
                pages = None
        except KeyError:
            pages = None
        volume = record["biblio"].get("volume")
        number = record["biblio"].get("issue")

        # TODO: Get Journal and ISBN

        metadata = {
            "title": title,
            "authors": authors,
            "year": year,
            "abstract": abstract,
            "pages": pages,
            "volume": volume,
            "number": number,
            "doi": doi,
        }
        parsed_data.append(metadata)
    parsed_data_df = pd.DataFrame(parsed_data)
    return parsed_data_df


def update_records(records_df, col_specs):
    """Find missing information from OpenAlex API using pyalex

    Parameters
    ----------
    records_df : pandas dataframe
        Dataframe of citation records
    col_specs : dict
        Column definitions for mapping column names to standardized names
    """
    # Clean dois in case not already cleaned as they will be used to retrieve record metadata
    records_df[col_specs["doi"]] = records_df[col_specs["doi"]].apply(clean_doi)

    # Make year column string type
    records_df["year"] = records_df["year"].astype("object")

    # Make missing values as NAN
    string_columns = records_df.select_dtypes("object").columns
    records_df[string_columns] = (
        records_df[string_columns]
        .fillna("")
        .applymap(lambda val: np.nan if len(val) == 0 else val)
    )

    # Find records where DOI is available and fields required for deduplication are missing
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

    doi_list = records_df[col_specs["doi"]][records_df["missing_data"]].values
    retrieved_records = retrieve_records(doi_list)
    retrieved_records_df = parse_metadata(retrieved_records)

    retrieved_records_df = (
        records_df[col_specs["doi"]]
        .reset_index()
        .merge(retrieved_records_df, on="doi")
        .set_index("record_id")
    )

    updated_records_df = records_df.combine_first(retrieved_records_df)

    return updated_records_df
