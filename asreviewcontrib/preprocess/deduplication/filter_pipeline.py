import logging

import numpy as np
import pandas as pd
from asreviewcontrib.preprocess.base import BasePipeline


class Filtr:
    """Class for creating filters for record pairs using similarity features

    The filter can be a standalone filter, which is applied to a single
    feature, such filter can be created using the "add" method giving
    'feature' and 'threshold' as arguments.
    Another option is to create combined filters, which are combined either
    using "AND" or using "OR" operators on the results of individual
    feature filters.
    """

    def __init__(self):
        self.filters = []
        self.filter_type = None

    def add(self, feature, threshold):
        """Add (feature,threshold) to a standalone filter"""
        self.filters.append((feature, threshold))
        self.filter_type = "standalone"

    def add_multiple(self, filter_list, filter_type):
        if self.filter_type == "standalone":
            raise ValueError("You cannot add multiple filters to a standalone filter")

        if self.filter_type is not None and filter_type != self.filter_type:
            raise ValueError(
                f"You cannot add a '{filter_type}' filter to a '{self.filter_type}' filter"
            )

        self.filter_type = filter_type
        self.filters.extend(filter_list)


class FilterPipeline(BasePipeline):
    """Class for creating filter pipeline for record pairs using similarity features"""

    def __init__(self):
        super(FilterPipeline, self).__init__()

    def add(self, name: str, filtr: Filtr) -> None:
        """Add a filter to the pipeline"""
        self._pipeline.append((name, filtr))

    def apply_pipe(self, pairs_df: pd.DataFrame) -> pd.DataFrame:
        """Apply filters in the pipeline and combine their results by union

        Parameters
        ----------
        pairs_df : pd.DataFrame
            Pairs dataframe created by indexing (blocking) with similarity features
        """
        if len(self._pipeline) == 0:
            raise ValueError(
                "You need to add filters to the pipeline using 'add' method "
                "before using apply_pipe"
            )

        results = np.zeros(pairs_df.shape[0])
        for name, filtr in self._pipeline:

            logging.info(f"Applying {name} filter of type {filtr.filter_type}...")
            if filtr.filter_type == "standalone":
                temp = (pairs_df[filtr.filters[0][0]] >= filtr.filters[0][1]).values

            if filtr.filter_type == "and":
                temp = np.ones(pairs_df.shape[0])
                for feature, threshold in filtr.filters:
                    temp *= (pairs_df[feature] >= threshold).values

            if filtr.filter_type == "or":
                temp = np.zeros(pairs_df.shape[0])
                for feature, threshold in filtr.filters:
                    temp += (pairs_df[feature] >= threshold).values

            results += temp

        return pairs_df.loc[np.where(results)[0]]
