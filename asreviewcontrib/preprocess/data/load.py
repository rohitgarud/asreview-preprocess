from asreview.data import load_data as asreview_load_data
from asreview.exceptions import BadFileFormatError
from asreviewcontrib.preprocess.config import DEDUPLICATION_COLUMN_DEFINITIONS
from asreviewcontrib.preprocess.io import io_utils, xml_reader


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
                input_filepath, reader=xml_reader.EndnoteXMLReader
            ).df
        except Exception:
            raise BadFileFormatError

    df, col_specs = io_utils._standardize_dataframe_for_deduplication(
        df, column_spec=DEDUPLICATION_COLUMN_DEFINITIONS
    )
    return df, col_specs
