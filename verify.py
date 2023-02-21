#!/usr/bin/env python3
# pylint: disable=logging-fstring-interpolation
"""
Snapshot and compare directories, e.g., to confirm that they're identical.
"""

import argparse
import hashlib
import json
import locale
import logging
import os
import sqlite3
import stat
import sys
from pathlib import Path
from typing import Any, Dict, NamedTuple, Tuple, Union

locale.setlocale(locale.LC_ALL, "")  # for formatting numbers

BUFFER_SIZE = 16 * 2**20  # 16 MiB

SimpleJSON = Union[int, float, str, Dict[str, Any]]


class FileInfo(NamedTuple):
    path: str
    mode: int
    uid: int
    gid: int
    mtime: Union[int, float]
    size: int
    digest: str


def make_digest(path: Path, mode: int) -> str:
    """
    Returns the SHA-1 hash for the given path.
    """

    hasher = hashlib.sha1()  # same algorithm as xfer.py  # nosec blacklist

    if stat.S_ISREG(mode):
        with open(path, "rb") as fp:
            while True:
                data = fp.read(BUFFER_SIZE)
                if not data:
                    break
                hasher.update(data)
    elif stat.S_ISLNK(mode):
        hasher.update(bytes(os.readlink(path), "utf-8"))

    return hasher.hexdigest()


def println(tag: str, data: SimpleJSON, file=sys.stdout) -> None:
    print(f"{tag} {json.dumps(data)}", file=file)


def parseln(line: str) -> Tuple[str, SimpleJSON]:
    tag, data = line.split(" ", maxsplit=1)
    return tag, json.loads(data)


# --------------------------------------------------------------------------


def do_hash(args: argparse.Namespace) -> None:
    bytes_last_reported = 0
    bytes_seen = 0
    dirs_seen = 0
    files_seen = 0
    out_file = sys.stdout

    if args.output:
        out_file = open(args.output, mode="w", encoding="utf-8")

    println("ROOT", args.root, file=out_file)

    for (dirpath, dirnames, filenames) in os.walk(args.root):
        dirnames[:] = sorted(dirnames)
        filenames[:] = sorted(filenames)

        dirpath = Path(dirpath)

        for p in dirnames + filenames:
            entry = dirpath / p
            info = os.lstat(entry)
            digest = make_digest(entry, info.st_mode)

            info_struct = FileInfo(
                path=os.fspath(entry),
                mode=info.st_mode,
                uid=info.st_uid,
                gid=info.st_gid,
                mtime=info.st_mtime,
                size=info.st_size,
                digest=digest,
            )

            if stat.S_ISDIR(info.st_mode):
                println("DIR", info_struct._asdict(), file=out_file)
                dirs_seen += 1
            else:
                println("FILE", info_struct._asdict(), file=out_file)
                files_seen += 1
                bytes_seen += info_struct.size

            if bytes_seen - bytes_last_reported >= 128 * 2**20:  # 128 MiB
                logging.info(
                    f"Directories: {dirs_seen:n}; Files: {files_seen:n}; Bytes: {bytes_seen:n}"
                )
                bytes_last_reported = bytes_seen

    logging.info(
        f"Directories: {dirs_seen:n}; Files: {files_seen:n}; Bytes: {bytes_seen:n}"
    )
    if args.output:
        out_file.close()


def do_convert(args: argparse.Namespace) -> None:
    db = sqlite3.connect(args.destination)

    db.execute(
        """
        CREATE TABLE data(
            path STR PRIMARY KEY,
            mode INT,
            uid INT,
            gid INT,
            mtime REAL,
            size INT,
            digest STR
        )
        """
    )

    rows_seen = 0

    with open(args.source, encoding="utf-8") as fp:
        line = fp.readline()
        root = Path(parseln(line)[1])  # type: ignore[arg-type]

        while True:
            line = fp.readline()
            if not line:
                break
            info = FileInfo(**parseln(line)[1])  # type: ignore[arg-type]
            path = Path(info.path)
            info = info._replace(path=os.fspath(path.relative_to(root)))

            db.execute("INSERT INTO data VALUES (?, ?, ?, ?, ?, ?, ?)", info)
            rows_seen += 1

            if rows_seen % 100000 == 0:
                db.commit()
                logging.info(f"Rows seen: {rows_seen}")

    db.commit()
    logging.info(f"Rows seen: {rows_seen}")
    db.close()


def do_compare(args: argparse.Namespace) -> None:
    db = sqlite3.connect(getattr(args, "destination-db"))
    out_file = sys.stdout

    if args.output:
        out_file = open(args.output, mode="w", encoding="utf-8")

    rows_seen = 0

    with open(getattr(args, "source-list"), encoding="utf-8") as fp:
        line = fp.readline()
        root = Path(parseln(line)[1])  # type: ignore[arg-type]

        while True:
            line = fp.readline()
            if not line:
                break
            tag, data = parseln(line)
            info = FileInfo(**data)  # type: ignore[arg-type]
            path = Path(info.path)
            info = info._replace(path=os.fspath(path.relative_to(root)))

            cursor = db.execute(
                "SELECT * FROM data WHERE path = ?", (info.path,)
            )
            rows = cursor.fetchall()

            if not rows:
                println("MISSING", info.path, file=out_file)
            elif len(rows) > 1:
                raise RuntimeError(f"Primary key failure: {info.path!r}")
            else:
                new_info = FileInfo(*rows[0])

                if info == new_info:
                    println("OK", info.path, file=out_file)
                else:
                    diff = []

                    for field in info._fields:
                        if getattr(info, field) != getattr(new_info, field):
                            diff.append(field)

                    if not (tag == "DIR" and diff == ["size"]):
                        println(
                            "MISMATCH",
                            {"path": info.path, "kind": tag, "diff": diff},
                            file=out_file,
                        )

            rows_seen += 1

            if rows_seen % 100000 == 0:
                logging.info(f"Rows seen: {rows_seen}")

    if args.output:
        out_file.close()
    db.close()


# --------------------------------------------------------------------------


def init_logging() -> None:
    logging.basicConfig(
        format="[%(asctime)s] %(levelname)s %(message)s",
        level=logging.ERROR,
        stream=sys.stderr,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.set_defaults(func=None)
    parser.set_defaults(verbose=False)

    subparsers = parser.add_subparsers()

    hash_parser = subparsers.add_parser("hash")
    hash_parser.add_argument("-o", "--output", metavar="file")
    hash_parser.add_argument("root")
    hash_parser.set_defaults(func=do_hash)

    convert_parser = subparsers.add_parser("convert")
    convert_parser.add_argument("source")
    convert_parser.add_argument("destination")
    convert_parser.set_defaults(func=do_convert)

    compare_parser = subparsers.add_parser("compare")
    compare_parser.add_argument("-o", "--output", metavar="file")
    compare_parser.add_argument("source-list")
    compare_parser.add_argument("destination-db")
    compare_parser.set_defaults(func=do_compare)

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.func:
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        args.func(args)
    else:
        raise RuntimeError("No action specified on the command line")


if __name__ == "__main__":
    try:
        init_logging()
        main()
    except Exception:  # pylint: disable=broad-except
        logging.exception("Uncaught exception")
        sys.exit(1)
