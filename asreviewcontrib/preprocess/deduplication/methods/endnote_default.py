from asreviewcontrib.preprocess.config import COLS_FOR_DEDUPE
from asreviewcontrib.preprocess.deduplication.base import BaseDedup
from asreviewcontrib.preprocess.deduplication.clean_pipeline import CleanPipeline
from asreviewcontrib.preprocess.io import io_utils


class ENDefaultDedup(BaseDedup):
    """Class for implementing default deduplication strategy used by Endnote"""

    def __init__(self):
        super(ENDefaultDedup, self).__init__()
        self.data_df = None
        self.col_specs = None

    def dedup(self, df, drop_duplicates=False):
        self.col_specs = io_utils._get_column_spec(df)
        self.data_df = df.copy().fillna("")[
            [self.col_specs[col] for col in COLS_FOR_DEDUPE]
        ]

        clean = CleanPipeline(include=["authors", "year", "title"])
        self.data_df = clean.apply_pipe(self.data_df)

        # Indexing - blocking
        # List of tuples of columns to combine for indexing
        concat_columns = [("authors", "year", "title")]
        block_cols = self._create_combined_columns(concat_columns)
        candidate_pairs = self._get_candidate_pairs(block_cols)

        # Comparing
        pairs_df = self._get_pairs_df(candidate_pairs)

        # Filtering
        # pairs are automatically filtered during blocking for
        # the Endnote default method based on only "authors",
        # "year" and "title"
        pairs_rids = self._get_pair_rids(pairs_df)
        groups_rids = self._get_groups_from_pairs(pairs_rids)

        # Deduplicate dataset
        df = self._dedup(df, groups_rids, drop_duplicates)

        return df
