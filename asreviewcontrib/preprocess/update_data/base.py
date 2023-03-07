from abc import ABC, abstractmethod


class BaseUpdater(ABC):
    """Abstract class for record metadata updater"""

    name = "base-updater"

    @abstractmethod
    def retrieve_metadata(doi_list):
        """Retrive metadata for the records using doi

        Records are either retrieved from local database if available
        or retrieved using API of freely available database from web

        Parameters
        ----------
        doi_list : list
            List of dois to retrieve metadata for

        Returns
        -------
        dict
            Dictionary of retrieved metadata with dois as keys
        """

        raise NotImplementedError

    @abstractmethod
    def parse_metadata(retrieved_metadata):
        """Parse metadata and create pandas dataframe with required columns

        Parameters
        ----------
        retrieved_metadata : dict
            Dictionary of metadata retrieved from OpenAlex

        Returns
        -------
        Pandas dataframe
            Dataframe of records with updated matadata
        """

        raise NotImplementedError
