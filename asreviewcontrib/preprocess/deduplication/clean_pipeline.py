import logging

from asreviewcontrib.preprocess.base import BasePipeline
from asreviewcontrib.preprocess.deduplication import dd_utils
from asreviewcontrib.preprocess.io import io_utils

clean_funcs = {
    "title": dd_utils.clean_title,
    "abstract": dd_utils.clean_abstract,
    "authors": dd_utils.clean_authors,
    "year": dd_utils.clean_year,
    "doi": dd_utils.clean_doi,
    "journal": dd_utils.clean_journal,
    "pages": dd_utils.clean_pages,
    "volume": dd_utils.clean_volume,
    "number": dd_utils.clean_number,
    "isbn": dd_utils.clean_isbn,
}


class CleanPipeline(BasePipeline):
    """Class for cleaning columns of the dataset"""

    def __init__(self, include=None, exclude=None):
        super(CleanPipeline, self).__init__()
        self.include = include
        self.exclude = exclude

    def add(self, col_name, function):
        """Add a cleaning function to the pipeline"""
        self.pipeline.append((col_name, function))

    def pipe(self, data_df):
        """Apply functions in the cleaning pipeline to the dataset

        Parameters
        ----------
        data_df : pandas dataframe
            uncleaned dataframe containing non uniform columns

        Returns
        -------
        pandas dataframe
            cleaned dataset with unified columns
        """
        data_df = data_df.copy().fillna("")
        col_specs = io_utils._get_column_spec(data_df)

        if self.include and self.exclude:
            logging.warning(
                "Both 'include' and 'exclude' are specified. Only 'include' will be used."
            )
        if self.include:
            apply_cleaning = self.include
        elif self.exclude:
            apply_cleaning = [
                clean for clean in clean_funcs if clean not in self.exclude
            ]
        else:
            apply_cleaning = list(clean_funcs.keys())

        # This takes care of the case where custom cleaning functions are
        # added to pipeline using 'add'
        for col_name in apply_cleaning:
            self.add(col_name, clean_funcs[col_name])

        for col, clean_func in self.pipeline:
            data_df[col_specs[col]] = data_df[col_specs[col]].apply(clean_func)

        data_df = data_df.sort_index()
        data_df[col_specs["year"]] = data_df[col_specs["year"]].astype("object")
        data_df = data_df.fillna("")

        return data_df
