"""Command line entry point.

    python -m dispatcher run      run the dispatcher until every lead is finished
"""
import argparse

from .main import main as run_main


def cli() -> None:
    parser = argparse.ArgumentParser(prog="python -m dispatcher",
                                     description="Durable campaign dispatcher on Temporal")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="run until every lead reaches a final state")
    args = parser.parse_args()
    if args.command == "run":
        run_main()


if __name__ == "__main__":
    cli()
