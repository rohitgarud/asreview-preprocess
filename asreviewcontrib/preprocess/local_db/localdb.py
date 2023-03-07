from tinydb import TinyDB, Query
from asreviewcontrib.preprocess.local_db.base import BaseLocalDB


class TinyLocalDB(BaseLocalDB):
    def __init__(self):
        super(TinyLocalDB, self).__init__()
        self.db_path = self._get_localdb_path()
        self.db = TinyDB(self.db_path)

    def retrieve_records(self, doi_list):
        """Retrieve records corresponding to dois in the doi_list
        from the local datatabase

        Parameters
        ----------
        doi_list : list
            list of dois in unified format

        Returns
        -------
        tuple
            tuple of:
            locally_retrieved_records (records available in localdb) and
            doi_list_to_retrieve (dois of records not available in localdb)
        """
        doi_list_to_retrieve = []
        locally_retrieved_records = {}

        Record = Query()
        for doi in doi_list:
            try:
                metadata = self.db.search(Record.doi == doi)[0]
                locally_retrieved_records[doi] = metadata
            except IndexError:
                doi_list_to_retrieve.append(doi)

        return locally_retrieved_records, doi_list_to_retrieve

    def add_records(self, retrieved_records, doi_list):
        """Add records retrieved from OpenAlex API to local database

        Parameters
        ----------
        retrieved_records : dict
            Dictionary of retrieved records with dois as keys
        doi_list : list
            List of dois
        """
        for doi in doi_list:
            try:
                self.db.insert(retrieved_records[doi])
            except KeyError:
                pass

    def _get_localdb_path(self):
        localdb_path = "./records.json"
        # TODO: Save local database in safe location for future use
        return localdb_path


# TODO: Add classes for other NoSQL databases like MongoDB and ElasticSearch
