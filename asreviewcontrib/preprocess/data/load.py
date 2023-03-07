from asreview.exceptions import BadFileFormatError
from asreview.data import load_data as asreview_load_data

from asreviewcontrib.preprocess.io import xml_reader, io_utils


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


# Load Data
def load_data(input_filepath):
    """Load data from file, URL or plugin.

    Parameters
    ----------
    input_filepath : str, pathlib.Path
        File path, URL, or alias of extension dataset.
        Supported file extensions are:
        .csv, .tab, .tsv, .xlsx, .ris, .txt and .xml (Endnote XML)
    """
    try:
        df = asreview_load_data(input_filepath).df
    except BadFileFormatError:
        try:
            df = asreview_load_data(
                input_filepath,
                reader=xml_reader.EndnoteXMLReader).df
        except Exception:
            raise BadFileFormatError

    df, col_specs = io_utils._standardize_dataframe_for_deduplication(
        df, column_spec=DEDUPLICATION_COLUMN_DEFINITIONS
    )
    return df, col_specs
