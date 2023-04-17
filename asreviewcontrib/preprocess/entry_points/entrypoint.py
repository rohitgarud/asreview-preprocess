import argparse
import os
from datetime import datetime

from asreview.entry_points import BaseEntryPoint
from asreviewcontrib.preprocess.deduplication.dedup import apply_dedup
from asreviewcontrib.preprocess.entry_points import ep_utils
from asreviewcontrib.preprocess.update_data.update import update_records

AVAILABLE_COMMANDS = ["dedup", "update"]
HOST_NAME = "localhost"
PORT_NUMBER = 5000


class PreprocessEntryPoint(BaseEntryPoint):
    description = "Preprocess records from input dataset including merging multiple datasets, finding missing data, deduplication"
    extension_name = "asreview-preprocess"

    @property
    def version(self):
        from asreviewcontrib.notes_export.__init__ import __version__

        return __version__

    def execute(self, argv):

        if argv[0] in AVAILABLE_COMMANDS:

            if argv[0] == "dedup":

                dedup_parser = argparse.ArgumentParser(prog="asreview preprocess dedup")

                dedup_parser.add_argument(
                    "input_path",
                    metavar="input_path",
                    type=str,
                    nargs="+",
                    help="The file path of the dataset.",
                )

                dedup_parser.add_argument(
                    "-m",
                    "--method",
                    dest="method",
                    default="asr",
                    type=str,
                    help="Method for deduplication (default: 'asr'). Methods available with CLI: ['asr', 'endnote']",
                )

                dedup_parser.add_argument(
                    "--pid",
                    default="doi",
                    type=str,
                    help="Persistent identifier used for deduplication (default: 'doi'). Currently only doi is supported",
                )

                dedup_parser.add_argument(
                    "--drop",
                    dest="drop_duplicates",
                    action="store_true",
                    help="Only export deduplicated records in the output file.",
                )

                dedup_parser.add_argument(
                    "-o",
                    "--output",
                    dest="output_path",
                    help="Output file path. Currently only .csv files are supported.",
                )

                dedup_args = dedup_parser.parse_args(argv[1:])

                if len(dedup_args.input_path) > 1:
                    raise ValueError(
                        "Deduplicating records from multiple files"
                        " via the CLI is not supported yet."
                    )

                input_path = dedup_args.input_path[0]
                output_path = ep_utils.get_output_path(dedup_args)

                apply_dedup(
                    input_path=input_path,
                    output_path=output_path,
                    method=dedup_args.method,
                    pid=dedup_args.pid,
                    drop_duplicates=dedup_args.drop_duplicates,
                )

            elif argv[0] == "update":
                update_parser = argparse.ArgumentParser(
                    prog="asreview preprocess update"
                )

                update_parser.add_argument(
                    "input_path",
                    metavar="input_path",
                    type=str,
                    nargs="+",
                    help="The file path of the dataset.",
                )

                update_parser.add_argument(
                    "-email",
                    "--email",
                    type=str,
                    help="Your email id. Missing data is found using Openalex API with pyalex package. Better speed and performance can be achieved by adding email to API call. See OpenAlex documentation for more details at https://docs.openalex.org/how-to-use-the-api/rate-limits-and-authentication",
                )

                update_parser.add_argument(
                    "--doi-updater",
                    dest="doi_updater",
                    default="crossref",
                    type=str,
                    help="Method for updating missing DOIs (default: crossref). Available [crossref]",
                )

                update_parser.add_argument(
                    "--data-updater",
                    dest="data_updater",
                    default="openalex",
                    type=str,
                    help="Method for updating missing metadata including abstracts (default: openalex). Available [openalex]",
                )

                update_parser.add_argument(
                    "--localdb",
                    dest="localdb",
                    default="tinydb",
                    type=str,
                    help="Method for saving retrieved matadata to local database (default: tinydb). Available [tinydb]",
                )

                update_parser.add_argument(
                    "-o",
                    "--output",
                    dest="output_path",
                    help="Output file path. Currently only .csv files are supported.",
                )

                update_args = update_parser.parse_args(argv[1:])

                if len(update_args.input_path) > 1:
                    raise ValueError(
                        "Finding abstracts for records from multiple files"
                        " via the CLI is not supported yet."
                    )

                input_path = update_args.input_path[0]
                output_path = ep_utils.get_output_path(update_args, after="updated")

                update_records(
                    input_path=input_path,
                    output_path=output_path,
                    email=update_args.email,
                    doi_update_method=update_args.doi_updater,
                    data_update_method=update_args.data_updater,
                    local_database=update_args.localdb,
                )

            else:
                raise ValueError(
                    f"The command {argv[0]} is not available. Please use one from {AVAILABLE_COMMANDS}"
                )

        else:
            parser = argparse.ArgumentParser(
                prog="asreview preprocess",
                formatter_class=argparse.RawTextHelpFormatter,
                description="Data preprocessing for ASReview.",
            )

            parser.add_argument(
                "subcommand",
                nargs="?",
                default=None,
                help=f"Available commands:\n\n {AVAILABLE_COMMANDS}",
            )

            parser.add_argument(
                "-V",
                "--version",
                action="version",
                version=f"{self.extension_name}: {self.version}",
            )

            args, _ = parser.parse_known_args()

            print(args)
