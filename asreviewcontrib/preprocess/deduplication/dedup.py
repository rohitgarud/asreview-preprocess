import pandas as pd
from asreviewcontrib.preprocess.utils import _deduplicator_class_from_entry_point


def apply_dedup(
    records_df: pd.DataFrame, method="asr", drop_duplicates=True
) -> pd.DataFrame:
    """Apply deduplication to remove duplicate records

    Parameters
    ----------
    records_df : pd.DataFrame
        Dataset with possible duplicate records
    method : str, optional
        deduplication method
        Available methods [asr, endnote], by default "asr"
    drop_duplicates : bool, optional
        Remove duplicate records from dataset, by default True
        if False, adds keep_remove column to dataset for indicating duplicates

    Returns
    -------
    pd.DataFrame
        _description_
    """
    deduplicator = _deduplicator_class_from_entry_point(method)

    return deduplicator.dedup(records_df, drop_duplicates=drop_duplicates)
