import pandas as pd
import xml.etree.ElementTree as ET

from asreview.io.utils import _standardize_dataframe


class EndnoteXMLReader:
    """Endnote XML file reader."""

    read_format = [".xml"]
    write_format = [".csv", ".tsv", ".xlsx"]

    @classmethod
    def read_data(cls, fp):
        """Import dataset.

        Arguments
        ---------
        fp: str, pathlib.Path
            File path to the XML file.

        Returns
        -------
        list:
            List with entries.
        """
        tree = ET.parse(fp)
        root = tree.getroot()
        dataset_list = []
        for i, record in enumerate(root[0]):
            record_id = record.find("rec-number").text
            try:
                ref_type = record.find("ref-type").attrib["name"]
            except:
                ref_type = None
            try:
                authors = ", ".join(
                    author[0].text
                    for author in record.find("contributors").find("authors")
                )
            except:
                authors = None
            try:
                title = record.find("titles").find("title")[0].text
            except:
                title = None
            try:
                second_title = record.find("titles").find("secondary-title")[0].text
            except:
                second_title = None
            try:
                journal = record.find("periodical").find("full-title")[0].text
            except:
                journal = None
            try:
                doi = record.find("electronic-resource-num")[0].text
            except:
                doi = None
            try:
                pages = record.find("pages")[0].text
            except:
                pages = None
            try:
                volume = record.find("volume")[0].text
            except:
                volume = None
            try:
                issue = record.find("number")[0].text
            except:
                issue = None
            try:
                year = record.find("dates").find("year")[0].text
            except:
                year = None
            try:
                url = record.find("urls").find("related-urls").find("url")[0].text
            except:
                url = None
            try:
                isbn = record.find("isbn")[0].text
            except:
                isbn = None
            try:
                abstract = record.find("abstract")[0].text
            except:
                abstract = None
            try:
                caption = record.find("caption")[0].text
            except:
                caption = None
            try:
                label = record.find("label")[0].text
            except:
                label = None
            dataset_list.append(
                {
                    "record_id": record_id,
                    "ref_type": ref_type,
                    "authors": authors,
                    "title": title,
                    "year": year,
                    "journal": journal,
                    "secondary_title": second_title,
                    "doi": doi,
                    "pages": pages,
                    "volume": volume,
                    "issue": issue,
                    "abstract": abstract,
                    "isbn": isbn,
                    "url": url,
                    "caption": caption,
                    "label": label,
                }
            )

        df = pd.DataFrame(dataset_list)
        df, _ = _standardize_dataframe(df)
        return df
