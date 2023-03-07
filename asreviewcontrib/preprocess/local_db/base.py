from abc import ABC, abstractmethod


class BaseLocalDB(ABC):
    """Abstract class for local database manager"""

    name = "base"

    @abstractmethod
    def retrieve_records_from_localdb(self, doi_list):
        """Retrieve records from local database from doi list"""

        raise NotImplementedError

    def update_localdb(self, retrieved_records, doi_list):
        """Add retrieved records to local database"""

        raise NotImplementedError
