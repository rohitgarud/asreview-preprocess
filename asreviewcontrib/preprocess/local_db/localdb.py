from tinydb import TinyDB, Query
from asreviewcontrib.preprocess.local_db.base import BaseLocalDB


class TinyLocalDB(BaseLocalDB):
    def __init__(
        self,
    ):
        super(TinyLocalDB, self).__init__()
        self.db = TinyDB("./records.json")

    def retrieve_records_from_localdb(self, doi_list):
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

    def update_localdb(self, retrieved_records, doi_list):

        for doi in doi_list:
            try:
                self.db.insert(retrieved_records[doi])
            except KeyError:
                pass
