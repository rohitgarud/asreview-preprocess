from asreviewcontrib.preprocess.deduplication.methods.asr import ASRDedup


def apply_dedup(records_df, method="asr"):
    """Apply deduplication to remove duplicate records

    Parameters
    ----------
    records_df : Pandas Dataframe
        Dataset with duplicate records
    method : str, optional
        deduplication method, by default "asr"
        Available methods [asr, endnote]

    Returns
    -------
    Pandas Dataframe
        Deduplicated Dataset
    """

    if method == "asr":
        deduplicator = ASRDedup()
    else:
        raise NotImplementedError

    return deduplicator.dedup(records_df, drop_duplicates=True)
