import pandas as pd
from pyalex import Works

from asreviewcontrib.preprocess.update_data.base import BaseUpdater

OPENALEX_QUERY_LIMIT = 25


class OpenAlexUpdater(BaseUpdater):
    """Class for updating records data by retrieving missing data from
    either local database if available or from OpenAlex database
    """

    name = "openalex-updater"

    def __init__(self, db):
        super(OpenAlexUpdater, self).__init__()
        self.db = db

    def retrieve_metadata(self, doi_list):
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
        # Try to retrieve records from local database if available
        retrieved_metadata, doi_list = self.db.retrieve_records(doi_list)

        doi_chunks = (
            doi_list[i : i + OPENALEX_QUERY_LIMIT]
            for i in range(0, len(doi_list), OPENALEX_QUERY_LIMIT)
        )

        # Retrieve records in chunks using OpenAlex "OR" syntax with pyalex
        for chunk in doi_chunks:
            data_chunk = Works().filter(doi="|".join(chunk)).get()
            for data in data_chunk:
                retrieved_metadata[data["doi"]] = data

        self.db.add_records(retrieved_metadata, doi_list)

        return retrieved_metadata

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
        parsed_data = []
        for doi, record in retrieved_metadata.items():
            title = record.get("title")
            authors = ", ".join(
                author["author"]["display_name"]
                for author in record.get("authorships", [])
            )
            year = record.get("publication_year")
            abstract = record.get("abstract")
            try:
                if record["biblio"].get("first_page"):
                    pages = f"{record['biblio']['first_page']}-{record['biblio']['last_page']}"
                else:
                    pages = None
            except KeyError:
                pages = None
            volume = record["biblio"].get("volume")
            number = record["biblio"].get("issue")

            # TODO: Get Journal and ISBN

            metadata = {
                "title": title,
                "authors": authors,
                "year": year,
                "abstract": abstract,
                "pages": pages,
                "volume": volume,
                "number": number,
                "doi": doi,
            }
            parsed_data.append(metadata)
        parsed_data_df = pd.DataFrame(parsed_data)
        return parsed_data_df


# TODO: Check if Lens.org api or database can be used freely
