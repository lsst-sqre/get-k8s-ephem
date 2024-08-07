"""CLI for looper and executor."""

import argparse
import datetime
import os
import time
from pathlib import Path

from .executor import Executor


def _parse_executor_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f", "--file", default=os.environ.get("K8S_EPHEM_FILE", None)
    )
    args = parser.parse_args()
    if args.file is not None:
        args.file = Path(args.file)
    return args


def _parse_looper_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--directory",
        "--dir",
        default=os.environ.get("K8S_EPHEM_DIR", "."),
    )
    parser.add_argument(
        "-i", "--interval", default=os.environ.get("K8S_EPHEM_INTERVAL", "600")
    )
    # Run for 30 days (at 10 minute intervals)
    parser.add_argument(
        "-l", "--loops", default=os.environ.get("K8S_EPHEM_LOOPS", "43200")
    )
    args = parser.parse_args()
    args.directory = Path(args.directory)
    i_int = int(args.interval)
    if i_int < 1:
        i_int = 1
    args.interval = i_int
    # No point in going through datetime.timedelta, since sleep just wants int
    l_int = int(args.loops)
    if l_int < 1:
        l_int = 1
    args.loops = l_int

    return args


def executor() -> None:
    """Run in single-shot mode."""
    args = _parse_executor_arguments()
    exc = Executor(file=args.file)
    exc.query()
    exc.report()


def looper() -> None:
    """Collect data over time."""
    args = _parse_looper_arguments()
    for _ in range(args.loops):
        then = datetime.datetime.now(tz=datetime.UTC)
        nowstr = then.isoformat(timespec="seconds")
        output = args.directory / f"{nowstr}-ephem.json"
        exc = Executor(file=output)
        exc.query()
        exc.report()
        now = datetime.datetime.now(tz=datetime.UTC)
        elapsed = int((now - then).total_seconds())
        remaining = args.interval - elapsed
        if remaining > 0:
            time.sleep(remaining)
