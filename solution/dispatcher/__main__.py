"""Command line entry point.

    python -m dispatcher run      run the dispatcher until every lead is finished,
                                  then write final_state.csv
    python -m dispatcher verify   compare final_state.csv with the provider's
                                  /admin/leads and show /admin/report
"""
import argparse
import sys

from .config import load_settings
from .main import main as run_main
from .output import print_verify, verify


def cli() -> None:
    parser = argparse.ArgumentParser(prog="python -m dispatcher",
                                     description="Durable campaign dispatcher on Temporal")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="run until every lead reaches a final state")
    sub.add_parser("verify", help="check final_state.csv against the provider")
    args = parser.parse_args()
    if args.command == "run":
        run_main()
    elif args.command == "verify":
        settings = load_settings()
        result = verify(settings.output_path, settings.provider_url)
        print_verify(result)
        sys.exit(0 if result.ok else 1)


if __name__ == "__main__":
    cli()
