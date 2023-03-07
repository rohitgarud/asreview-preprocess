import os
from datetime import datetime


def get_output_path(args):
    """Get output path based on user input.

    If path is given, check if it is accepted format.
    If path is not given, output filename is same as input filename
    with datetime added"""

    input_path = args.input_path[0]
    if args.output_path:
        output_path = args.output_path
        if not output_path.endswith(".csv"):
            if "." in output_path:
                raise ValueError(
                    "Output File extensions other than .csv are not supported yet"
                )
            else:
                output_path += ".csv"
    else:
        output_path = os.path.basename(input_path)
        output_path = f"{os.path.splitext(output_path)[0]}-deduplicated-{datetime.now().strftime('%Y%m%dT%H%M')}.csv"
    return output_path
