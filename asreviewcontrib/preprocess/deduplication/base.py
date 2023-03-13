from abc import ABC, abstractmethod
from itertools import combinations

import numpy as np
import pandas as pd
import recordlinkage
from asreviewcontrib.preprocess.config import COLS_FOR_DEDUPE

PAIRS_COLUMNS = [
    "record_id1",
    "record_id2",
    "authors1",
    "authors2",
    "authors",
    "title1",
    "title2",
    "title",
    "abstract1",
    "abstract2",
    "abstract",
    "doi1",
    "doi2",
    "doi",
    "year1",
    "year2",
    "year",
    "journal1",
    "journal2",
    "journal",
    "number1",
    "number2",
    "number",
    "pages1",
    "pages2",
    "pages",
    "volume1",
    "volume2",
    "volume",
    "isbn1",
    "isbn2",
    "isbn",
]


class BaseDedup(ABC):
    """Abstract class for deduplication methods"""

    @abstractmethod
    def dedup(self, data_df, drop_duplicates=True):

        raise NotImplementedError

    def _create_combined_columns(
        self,
        concat_columns,
    ):
        """Create combined columns by concatenation"""
        combined_col_names = []
        for ind, columns in enumerate(concat_columns):
            columns = [self.col_specs[col] for col in columns]
            new_col_name = f"{'_'.join(columns)}"
            self.data_df[new_col_name] = (
                self.data_df[columns].astype(str).fillna("").agg(" ".join, axis=1)
            )
            combined_col_names.append(new_col_name)

        return combined_col_names

    def _get_candidate_links(self, block_cols):
        """Get candidate links using records linkage blocking method of indexing"""
        # Replacing empty strings with NA so that Record Linkage doesnot
        # consider two empty strings as a match while blocking
        string_columns = self.data_df.select_dtypes("object").columns
        self.data_df[string_columns] = (
            self.data_df[string_columns]
            .fillna("")
            .applymap(lambda val: np.nan if len(val) == 0 else val)
        )

        # Indexing using blocking
        indexer = recordlinkage.Index()
        for col in block_cols:
            indexer.block(left_on=col)
        candidate_links = indexer.index(self.data_df)

        return candidate_links

    def _get_similarity_features(self, candidate_links, method="jarowinkler"):
        """Calculate similarity metrics for all columns for candidate pairs
        using given method (default = jarowinkler)"""
        compare_cl = recordlinkage.Compare()
        for col in COLS_FOR_DEDUPE:
            compare_cl.string(
                self.col_specs[col],
                self.col_specs[col],
                method=method,
                label=col,
            )
        features = compare_cl.compute(candidate_links, self.data_df)

        return features

    def _get_pairs_w_features(self, candidate_links, features):
        """Create pairs dataframe with single row having values of different
        columns for the records in the pair and their corresponding similarity
        features"""
        pairs_df = pd.DataFrame()

        pairs_df["record_id1"] = [id1 for id1, _ in candidate_links]
        pairs_df["record_id2"] = [id2 for _, id2 in candidate_links]

        idx1 = [id1 - 1 for id1, _ in candidate_links]
        idx2 = [id2 - 1 for _, id2 in candidate_links]
        for col in COLS_FOR_DEDUPE:
            pairs_df[f"{col}1"] = self.data_df[self.col_specs[col]].values[idx1]
            pairs_df[f"{col}2"] = self.data_df[self.col_specs[col]].values[idx2]

        pairs_df = pairs_df.join(features.reset_index())
        pairs_df = pairs_df[PAIRS_COLUMNS]

        return pairs_df

    def _handle_missing_feature_values(self, pairs_df):
        """Handle missing feature values based on column values of pairs
        in pairs dataframe"""
        pairs_df["abstract"] = np.where(
            pairs_df["abstract1"].isna() & pairs_df["abstract2"].isna(),
            0,
            pairs_df["abstract"],
        )
        pairs_df["pages"] = np.where(
            pairs_df["pages1"].isna() & pairs_df["pages2"].isna(), 1, pairs_df["pages"]
        )
        pairs_df["volume"] = np.where(
            pairs_df["volume1"].isna() & pairs_df["volume2"].isna(),
            1,
            pairs_df["volume"],
        )
        pairs_df["number"] = np.where(
            pairs_df["number1"].isna() & pairs_df["number2"].isna(),
            1,
            pairs_df["number"],
        )
        pairs_df["doi"] = np.where(
            pairs_df["doi1"].isna() & pairs_df["doi2"].isna(), 0, pairs_df["doi"]
        )
        pairs_df["isbn"] = np.where(
            pairs_df["isbn1"].isna() & pairs_df["isbn2"].isna(), 0, pairs_df["isbn"]
        )

        return pairs_df

    def _get_pair_rids(pairs_df):
        """Get record_ids of pairs from the pairs dataframe"""
        pairs_rids = [
            tuple(sorted(pair))
            for pair in pairs_df[["record_id1", "record_id2"]].values
        ]

        return pairs_rids

    def _get_groups_from_pairs(pair_rids):
        """Get groups of duplicate records from pairs"""
        groups = []

        for id1, id2 in pair_rids:
            added = False
            for g in groups:
                if id1 in g and id2 not in g:
                    g.append(id2)
                    added = True
                    break
                elif id2 in g and id1 not in g:
                    g.append(id1)
                    added = True
                    break
                elif id1 in g and id2 in g:
                    added = True
            if not added:
                groups.append([id1, id2])

        groups = [tuple(sorted(group)) for group in groups]
        return groups

    def _get_pairs_from_groups(groups):
        """Get all pairs of duplicate records from groups"""
        pairs = []

        for group in groups:
            combs = [tuple(sorted(pair)) for pair in list(combinations(group, 2))]
            pairs.extend(combs)

        return pairs

    def _dedup(self, df, groups_rids, drop_duplicates):
        """Deduplicate dataset using final matching groups"""
        duplicate_rids = [rid for group in groups_rids for rid in group]
        unique_rids = [rid for rid in df.index.values if rid not in duplicate_rids]
        arrange_rids = duplicate_rids + unique_rids
        group_tags = [
            tag for i, group in enumerate(groups_rids) for tag in [i + 1] * len(group)
        ] + [0] * len(unique_rids)

        df = df.loc[arrange_rids]
        df["duplicate_group_id"] = group_tags
        df = df.sort_values(["duplicate_group_id", "abstract", "year"], ascending=False)

        groups_seen = []
        keep_remove = []

        for tag in df["duplicate_group_id"].values:
            if tag == 0:
                keep_remove.append("KEEP")
            elif tag in groups_seen:
                keep_remove.append("REMOVE")
            else:
                keep_remove.append("KEEP")
                groups_seen.append(tag)

        df["keep_remove"] = keep_remove

        if drop_duplicates:
            df = df.loc[np.where(df["keep_remove"] == "KEEP")].drop(
                "keep_remove", axis=1
            )

        return df
