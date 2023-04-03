import pandas as pd
from asreviewcontrib.preprocess.data.load import load_data
from asreviewcontrib.preprocess.utils import _deduplicator_class_from_entry_point


def apply_dedup(
    input_path, output_path, method="asr", drop_duplicates=False
) -> pd.DataFrame:
    """Apply deduplication to remove duplicate records

    Parameters
    ----------
    input_path: str
        Path of the input dataset
    output_path: str
        Path to save the deduplicated dataset
    method : str, optional
        deduplication method
        Available methods [asr, endnote], by default "asr"
    drop_duplicates : bool, optional
        Remove duplicate records from dataset, by default False
        if False, adds keep_remove column to dataset for indicating duplicates

    Returns
    -------
    pd.DataFrame
        Deduplicated dataset
    """

    records_df, _ = load_data(input_path)
    deduplicator = _deduplicator_class_from_entry_point(method)
    output_df = deduplicator.dedup(records_df, drop_duplicates=drop_duplicates)
    output_df.to_csv(output_path)
    print(f"Deduplicated dataset saved to {output_path}")
    return output_df
