COLS_FOR_DEDUPE = [
    "authors",
    "title",
    "abstract",
    "year",
    "journal",
    "doi",
    "volume",
    "pages",
    "number",
    "isbn",
]

# Additional column definitions required for deduplication
DEDUPLICATION_COLUMN_DEFINITIONS = {
    "record_id": ["record_id", "rec-number"],
    "year": ["year"],
    "ref_type": ["ref_type", "type_of_reference"],
    "journal": ["journal"],
    "volume": ["volume"],
    "pages": ["pages", "start_page"],
    "number": ["number", "issue"],
    "isbn": ["isbn", "issn"],
    "secondary_title": ["secondary_title", "secondary title", "secondary-title"],
}

OPENALEX_QUERY_LIMIT = 25
