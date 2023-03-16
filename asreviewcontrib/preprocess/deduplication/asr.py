from asreviewcontrib.preprocess.config import COLS_FOR_DEDUPE
from asreviewcontrib.preprocess.deduplication.base import BaseDedup
from asreviewcontrib.preprocess.deduplication.clean_pipeline import CleanPipeline
from asreviewcontrib.preprocess.io import io_utils


class ASRDedup(BaseDedup):
    """Class for implementing deduplication strategy used by ASReview Datatools

    Currently only working with dois as pid
    """

    def __init__(self):
        super(ASRDedup, self).__init__()
        self.data_df = None
        self.col_specs = None

    def dedup(self, df, drop_duplicates=False):
        self.col_specs = io_utils._get_column_spec(df)
        self.data_df = df.copy().fillna("")[
            [self.col_specs[col] for col in COLS_FOR_DEDUPE]
        ]

        clean = CleanPipeline(include=["title", "abstract", "doi"])
        self.data_df = clean.apply_pipe(self.data_df)

        # Indexing - blocking
        # List of tuples of columns to combine for indexing
        concat_columns = [("title", "abstract")]
        combined_col_names = self._create_combined_columns(concat_columns)
        block_cols = combined_col_names.append(self.col_specs["doi"])
        candidate_links = self._get_candidate_links(self, block_cols)

        # Comparing
        features = self._get_similarity_features(candidate_links)
        pairs_df = self._get_pairs_w_features(candidate_links, features)
        pairs_df = self._handle_missing_feature_values(pairs_df)

        # Filtering
        # pairs are automatically filtered during blocking for this method
        pairs_rids = self._get_pair_rids(pairs_df)
        groups_rids = self._get_groups_from_pairs(pairs_rids)

        # Deduplicate dataset
        df = self._dedup(df, groups_rids, drop_duplicates)

        return df
