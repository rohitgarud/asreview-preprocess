import csv
import logging
import re

from unidecode import unidecode

HTML_ENTITIES = [
    r"</\w+>",
    r"<\w+>",
    "&nbsp;",
    "&lt;",
    "&gt;",
    "&amp;",
    "&quot;",
    "&apos;",
    "&cent;",
    "&pound;",
    "&yen;",
    "&euro;",
    "&copy;",
    "&reg;",
]

# Dictionary of journal name abbreviations and full forms
with open("all_journal_abbreviations.csv", "r") as f:
    reader = csv.reader(f)
    all_journal_abbr = {}

    for row in reader:
        all_journal_abbr[row[0]] = row[1].encode("ascii", "ignore").decode()
    # TODO: Create SQL database of journal abbreviations


# Clean different fields to unify them to a common format
def clean_title(title):
    for entity in HTML_ENTITIES:
        title = re.sub(f"{entity}", " ", title)
    title = unidecode(title)
    title = re.sub(r"[^A-Za-z0-9]", " ", title)
    title = re.sub(r" +", " ", title).strip().upper()
    return title


def clean_abstract(abstract):
    for entity in HTML_ENTITIES:
        abstract = re.sub(f"{entity}", " ", abstract)
    abstract = unidecode(abstract)
    abstract = re.sub(r"[^A-Za-z0-9]", " ", abstract)
    abstract = re.sub(r" +", " ", abstract).strip().upper()
    # Remove copywrite information
    # abstract = re.sub(r"COPYRIGHT.*", " ", abstract)
    return abstract


def clean_year(year):
    """Unify year to a common format."""
    # TODO: add some checks to year
    return year


def clean_doi(doi):
    """unify DOIs to a common format https://doi.org/{doi}"""
    if len(doi) > 0:
        doi = re.findall(r"(10\..+)", doi)
        if len(doi) > 0:
            return f"https://doi.org/{doi[0].strip().upper()}"
    return doi


def clean_pages(pages):
    """Unify page numbers to a common format.
    Changes formats like 311-7 to 311-317."""

    # Checking if date is missfilled as pages in input dataset
    if len(re.findall(r"\d{2}-\d{2}-\d{4}", pages)) > 0:
        return ""

    if pages:
        pages = re.sub(r"[^0-9-]", "", pages)
        start, end = re.findall(r"(?P<start>[0-9]*)-?(?P<end>[0-9]*)", pages)[0]
        if end:
            len_diff = len(start) - len(end)
            if len_diff > 0:
                end = f"{start[:len_diff]}{end}"
            return f"{start}-{end}"
        return start


def clean_journal(journal):
    """Expand abbreviated journal names"""
    preprocess_journal = journal.encode("ascii", "ignore").decode("ascii")
    preprocess_journal = re.sub(r"[^a-zA-Z0-9\s]", "", preprocess_journal)
    # TODO: Handle Accents better

    try:
        return all_journal_abbr[preprocess_journal]
    except KeyError:
        return journal


def clean_authors(authors):
    """Unify author names to a common format."""
    authors = unidecode(authors)
    matches = re.finditer(
        r"\s?(?P<last>([A-Z]?[a-z]*\s?)*-?[A-Z][a-z]+),?\s+(?P<first>[A-Z])(?P<full>[a-z])?.?\s?(?P<middle>[A-Z])?.?",
        authors.replace('"', ""),
    )
    authors = " ".join(
        f"{match.group('last')} {match.group('first')} {match.group('middle')}"
        if match.group("middle")
        else f"{match.group('last')} {match.group('first')}"
        for match in matches
    )
    return authors


def clean_volume(volume):
    """Unify volume to a common format."""
    volume = volume.lower().replace("(no pagination)", "")
    return volume


def clean_number(number):
    """Unify number to a common format."""
    # TODO: remove unnecessary things from numbers
    return number


def clean_isbn(isbn):
    """Unify ISBNs/ISSNs to a common format."""
    isbn = re.sub(r"\s\((Print|Electronic)\).*", "", isbn)
    isbn = re.sub(r"\r", "; ", isbn)
    return isbn


def get_first_author(authors):
    first_author = []
    for item in authors.split():
        if len(item) == 1:
            break
        first_author.append(item)
    first_author = "".join(first_author)
    first_author = re.sub(r"[^A-Za-z]", "", first_author)
    return first_author.upper()


def get_short_title(title):
    short_title = []
    for word in title.split():
        if len(word) > 2:
            short_title.append(word)
        if len(short_title) == 3:
            break
    short_title = "".join(short_title)
    short_title = re.sub(r"[^A-Za-z]", "", short_title)
    return short_title.upper()
