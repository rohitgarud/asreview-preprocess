import argparse
import os
from datetime import datetime

from asreview.entry_points import BaseEntryPoint

from asreviewcontrib.preprocess.deduplication import deduplication
from asreviewcontrib.preprocess.gui import launch_gui


DATATOOLS = ["launch_GUI", "dedup"]


class PreprocessEntryPoint(BaseEntryPoint):
    description = "Preprocess records from input dataset including deduplication, finding missing abstracts"
    extension_name = "asreview-preprocess"

    @property
    def version(self):
        from asreviewcontrib.notes_export.__init__ import __version__

        return __version__

    def execute(self, argv):
        if len(argv) > 1 and argv[0] in DATATOOLS:

            if argv[0] == "launch_GUI":
                launch_gui()

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

                args = dedup_parser.parse_args(argv)

                if len(args.input_path) > 1:
                    raise ValueError(
                        "Deduplicating records from multiple files"
                        " via the CLI is not supported yet."
                    )

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

                deduplication(
                    input_path=input_path,
                    output_path=output_path,
                    method=args.methods,
                    pid=args.pid,
                    drop_duplicates=args.drop_duplicates,
                )

            if argv[0] == "abstract_finder":
                pass

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
                help=f"Available commands:\n\n" f"{DATATOOLS}",
            )

            parser.add_argument(
                "-V",
                "--version",
                action="version",
                version=f"{self.extension_name}: {self.version}",
            )

            args, _ = parser.parse_known_args()

            print(args)
