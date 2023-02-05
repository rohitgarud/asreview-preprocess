import pandas as pd
import numpy as np
from tqdm import tqdm

try:
    import pyalex
    from pyalex import Works
except ImportError:
    print(
        "The abstract finder requires additional packages to be installed. Install optional ASReview-preprocess dependencies specific for abstract finder with 'pip install asreview-preprocess[abstract_finder]' or all dependencies with 'pip install asreview-preprocess[all]'"
    )


from asreview.data import load_data
from asreviewcontrib.preprocess.preprocess_utils import clean_doi


def find_missing_abstracts(input_path, output_path, doi_column, email):
    dataset = load_data(input_path).df

    if email:
        pyalex.config.email = email

    dataset[doi_column] = dataset[doi_column].fillna("").apply(clean_doi)

    dataset["is_abstract_missing"] = (dataset["abstract"].str.len() <= 10).astype(int)
    n_missing_abstracts = dataset["is_abstract_missing"].sum()

    dois = dataset[doi_column].fillna("").apply(clean_doi).values
    abstracts = dataset["abstract"].values

    dataset["abstract"] = [
        abstract if len(abstract) > 10 or not doi else Works()[doi]["abstract"]
        for abstract, doi in tqdm(
            zip(abstracts, dois),
            desc="Scanning Records and Finding Missing Abstracts..",
            unit=" records",
        )
    ]

    # TODO: Get multiple records in single query using OR SYNTAX https://blog.ourresearch.org/fetch-multiple-dois-in-one-openalex-api-request/

    n_missing_abstracts_remain = (dataset["abstract"].str.len() <= 10).sum()
    n_found_abstracts = n_missing_abstracts - n_missing_abstracts_remain

    print(f"Total number of records: {len(dataset)}")
    print(f"Number of records with missing abstracts: {n_missing_abstracts}")
    print(f"Number of abstracts found: {n_found_abstracts}")
    print(
        f"Number of records with missing abstracts remaining: {n_missing_abstracts_remain}"
    )
    print("Abstract finding process completed.")

    dataset.to_csv(output_path, index=False)
