import re
import os
from datetime import datetime


def clean_doi(doi):
    pattern = re.compile(r"(https?://(dx.)?doi\.org/)?(.*)")
    if doi:
        return f"https://doi.org/{pattern.match(doi).group(3)}"
    return doi


def clean_authors(authors):
    pass


def get_output_path(args):
    input_path = args.input_path[0]
    if args.output_path:
        output_path = args.output_path
        if not output_path.endswith(".csv"):
            if "." in output_path:
                raise ValueError(
                    "File extensions other than .csv are not supported yet"
                )
            else:
                output_path += ".csv"
    else:
        output_path = os.path.basename(input_path)
        output_path = f"{os.path.splitext(output_path)[0]}-deduplicated-{datetime.now().strftime('%Y%m%dT%H%M')}.csv"
    return output_path
