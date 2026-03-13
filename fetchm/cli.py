import argparse

from fetchm.metadata import build_metadata_parser, run_metadata_pipeline
from fetchm.sequence import build_sequence_parser, run_sequence_downloads


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fetchm",
        description="Unified metadata and sequence download CLI for fetchm.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    metadata_parser = subparsers.add_parser(
        "metadata",
        parents=[build_metadata_parser(add_help=False)],
        help="Fetch metadata and generate summaries from an NCBI dataset TSV.",
    )
    metadata_parser.set_defaults(func=run_metadata_pipeline)

    run_parser = subparsers.add_parser(
        "run",
        parents=[build_metadata_parser(add_help=False)],
        help="Run metadata generation and sequence download in one command.",
    )
    run_parser.set_defaults(func=run_metadata_pipeline, seq=True)

    seq_parser = subparsers.add_parser(
        "seq",
        parents=[build_sequence_parser(add_help=False)],
        help="Download genome FASTA files from ncbi_clean.csv.",
    )
    seq_parser.set_defaults(func=run_sequence_downloads)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
