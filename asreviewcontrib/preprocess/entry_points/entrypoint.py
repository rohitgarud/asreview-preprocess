import argparse
import os
from datetime import datetime

from asreview.entry_points import BaseEntryPoint

from asreviewcontrib.preprocess.deduplication import deduplication
from asreviewcontrib.preprocess.abstract_finder import find_missing_abstracts
from asreviewcontrib.preprocess.webapp.gui import launch_gui
from asreviewcontrib.preprocess.entry_points import ep_utils

AVAILABLE_COMMANDS = ["launch_gui", "dedup", "abstract_finder"]
HOST_NAME = "localhost"
PORT_NUMBER = 5000


class PreprocessEntryPoint(BaseEntryPoint):
    description = "Preprocess records from input dataset including deduplication, finding missing abstracts"
    extension_name = "asreview-preprocess"

    @property
    def version(self):
        from asreviewcontrib.notes_export.__init__ import __version__

        return __version__

    def execute(self, argv):

        if argv[0] in AVAILABLE_COMMANDS:

            if argv[0] == "launch_gui":
                gui_parser = argparse.ArgumentParser(
                    prog="asreview preprocess launch_gui",
                    description="ASReview Preprocess GUI",
                )

                gui_parser.add_argument(
                    "--ip",
                    default=HOST_NAME,
                    type=str,
                    help="The IP address the server will listen on.",
                )

                gui_parser.add_argument(
                    "--port",
                    default=PORT_NUMBER,
                    type=int,
                    help="The port the server will listen on.",
                )

                gui_parser.add_argument(
                    "--no-browser",
                    dest="no_browser",
                    action="store_true",
                    help="Do not open ASReview Preprocess GUI in a browser after startup.",
                )

                gui_parser.add_argument(
                    "--port-retries",
                    dest="port_retries",
                    default=50,
                    type=int,
                    help="The number of additional ports to try if the"
                    "specified port is not available.",
                )

                gui_parser.add_argument(
                    "--certfile",
                    default="",
                    type=str,
                    help="The full path to an SSL/TLS certificate file.",
                )

                gui_parser.add_argument(
                    "--keyfile",
                    default="",
                    type=str,
                    help="The full path to a private key file for usage with SSL/TLS.",
                )

                gui_args = gui_parser.parse_args(argv[1:])

                launch_gui(gui_args)

            elif argv[0] == "dedup":

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
                    dest="methods",
                    default=["title"],
                    type=str,
                    help="Methods for deduplication (default: 'title'). Methods available with CLI: ['title','abstract','pid']. Give multiple methods as -m title -m abstract -m pid. Additional methods are available in GUI. Use 'asreview preprocess launch_GUI' to start GUI",
                )

                dedup_parser.add_argument(
                    "--pid",
                    default="doi",
                    type=str,
                    help="Persistent identifier used for deduplication (default: 'doi'). Only used if method is 'pid'",
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

                dedup_args = dedup_parser.parse_args(argv)

                if len(dedup_args.input_path) > 1:
                    raise ValueError(
                        "Deduplicating records from multiple files"
                        " via the CLI is not supported yet."
                    )

                input_path = dedup_args.input_path[0]
                output_path = ep_utils.get_output_path(dedup_args)

                deduplication(
                    input_path=input_path,
                    output_path=output_path,
                    method=dedup_args.methods,
                    pid=dedup_args.pid,
                    drop_duplicates=dedup_args.drop_duplicates,
                )

            elif argv[0] == "abstract_finder":
                af_parser = argparse.ArgumentParser(
                    prog="asreview preprocess abstract_finder"
                )

                af_parser.add_argument(
                    "input_path",
                    metavar="input_path",
                    type=str,
                    nargs="+",
                    help="The file path of the dataset.",
                )

                af_parser.add_argument(
                    "--doi_column",
                    default="doi",
                    type=str,
                    help="Name of DOI column, as dois will be used for finding abstracts (default: doi)",
                )

                af_parser.add_argument(
                    "-email",
                    "--email",
                    type=str,
                    help="Your email id. Abstracts are found using Openalex API with pyalex package. Better speed and performance can be achieved by adding email to API call. See OpenAlex documentation for more details at https://docs.openalex.org/how-to-use-the-api/rate-limits-and-authentication",
                )

                af_parser.add_argument(
                    "-o",
                    "--output",
                    dest="output_path",
                    help="Output file path. Currently only .csv files are supported.",
                )

                af_args = af_parser.parse_args(argv[1:])

                if len(af_args.input_path) > 1:
                    raise ValueError(
                        "Finding abstracts for records from multiple files"
                        " via the CLI is not supported yet."
                    )

                input_path = af_args.input_path[0]
                output_path = ep_utils.get_output_path(af_args)

                find_missing_abstracts(
                    input_path=input_path,
                    output_path=output_path,
                    doi_column=af_args.doi_column,
                    email=af_args.email,
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
                help=f"Available commands:\n\n" f"{AVAILABLE_COMMANDS}",
            )

            parser.add_argument(
                "-V",
                "--version",
                action="version",
                version=f"{self.extension_name}: {self.version}",
            )

            args, _ = parser.parse_known_args()

            print(args)
