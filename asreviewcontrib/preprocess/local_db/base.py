from abc import ABC, abstractmethod


class BaseLocalDB(ABC):
    """Abstract class for local database manager"""

    name = "base"

    @abstractmethod
    def retrieve_records(self, doi_list):
        """Retrieve records from local database from doi list"""

        raise NotImplementedError

    @abstractmethod
    def add_records(self, retrieved_records, doi_list):
        """Add retrieved records to local database"""

        raise NotImplementedError

    @abstractmethod
    def _get_localdb_path(self):
        """Get path where the local database is saved"""

        raise NotImplementedError
