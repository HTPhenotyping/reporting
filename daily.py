#!/usr/bin/env python3
"""
Reporting on HT Phenotyping's storage.
"""

import collections
import csv
import datetime
import email.mime.multipart
import email.mime.text
import json
import logging
import pathlib
import smtplib
import ssl
import sys
import textwrap
import time
from typing import Any, Dict, List, Optional, Tuple

import html2text
import minio  # type: ignore[import]

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent

CONFIG_FILE = SCRIPT_DIR / "secrets" / "config.csv"
S3_CONFIG_FILE = SCRIPT_DIR / "secrets" / "s3_config.json"
SNAPSHOT_DIR = SCRIPT_DIR / "snapshots"

TO = ["baydemir@morgridge.org"]  # ["g2fuas_support@g-groups.wisc.edu"]
CC = ["baydemir@morgridge.org"]
FROM = "htphytomorph@botany.wisc.edu"
REPLY_TO = "baydemir@morgridge.org"  # "g2fuas_support@g-groups.wisc.edu"

JSON = Dict[str, Any]
Snapshot = Dict[str, Dict[str, Any]]


def get_config() -> "List[collections.OrderedDict[str, str]]":
    """
    Returns the list of collaborators and where their data is located.
    """

    with open(CONFIG_FILE, encoding="utf-8", newline="") as fp:
        return list(csv.DictReader(fp))


def get_si_suffix(n: int) -> Tuple[float, str]:
    """
    Converts the given number of bytes to a human-readable form.
    """

    suffixes = ["&nbsp;&nbsp;B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    b = float(n)
    i = 0
    while abs(b) > 1000 and i < len(suffixes) - 1:
        b /= 1024
        i += 1
    return (b, suffixes[i])


def get_s3_client() -> minio.Minio:
    """
    Returns a client for interacting with S3.
    """

    with open(S3_CONFIG_FILE, encoding="utf-8") as fp:
        config = json.load(fp)
    return minio.Minio(
        config["url"],
        access_key=config["accessKey"],
        secret_key=config["secretKey"],
    )


def scan_s3_bucket(
    name: str,
    client: Optional[minio.Minio] = None,
) -> Tuple[int, int]:
    """
    Returns the total number of files and bytes in the given S3 bucket.
    """

    client = client or get_s3_client()
    num_files = 0
    num_bytes = 0

    for obj in client.list_objects(name, recursive=True):
        if not obj.is_dir:
            num_files += 1
            num_bytes += obj.size

    return (num_files, num_bytes)


def get_snapshot(lookback: int = 0) -> Snapshot:
    """
    Returns the snapshot from the given number of days ago.

    Raises `FileNotFoundError` if it does not exist.
    """

    today = datetime.datetime.now()
    target = (today - datetime.timedelta(days=lookback)).strftime("%Y-%m-%d")

    with open(SNAPSHOT_DIR / f"{target}.json", encoding="utf-8") as fp:
        return json.load(fp)  # type: ignore[no-any-return]


def get_previous_snapshot() -> Snapshot:
    """
    Returns the most recent snapshot from within the past 90 days.

    Returns an empty `dict` if one cannot be found.
    """

    for i in range(1, 91):
        try:
            return get_snapshot(lookback=i)
        except FileNotFoundError:
            pass
    return {}


def get_current_snapshot() -> Snapshot:
    """
    Builds a current snapshot of HT Phenotyping's storage.
    """

    snapshot: Snapshot = {"*": {"start_time": time.time()}}
    s3_client = None

    for collab in get_config():
        name = collab["name"]
        s3_bucket = collab["s3_bucket"]
        s3_files = 0
        s3_bytes = 0

        if s3_bucket:
            s3_client = s3_client or get_s3_client()
            (s3_files, s3_bytes) = scan_s3_bucket(s3_bucket, client=s3_client)

        snapshot[name] = {
            "s3_bucket": s3_bucket,
            "s3_files": s3_files,
            "s3_bytes": s3_bytes,
        }

    snapshot["*"]["end_time"] = time.time()

    return snapshot


def save_snapshot(data: Snapshot) -> None:
    """
    Writes the given snapshot to disk.
    """

    now = data["*"]["start_time"]
    today = datetime.datetime.fromtimestamp(now).strftime("%Y-%m-%d")

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    with open(SNAPSHOT_DIR / f"{today}.json", encoding="utf-8", mode="w") as fp:
        json.dump(data, fp, indent=2)


def get_html_message(
    previous: Snapshot,
    current: Snapshot,
) -> Tuple[str, int, int]:
    """
    Compares the two snapshots and returns an html message and summary data.
    """

    message = ""

    num_style = "font-family: monospace; text-align: right;"
    padding = "padding-left: 0.5em; padding-right: 0.5em;"
    table_start = textwrap.dedent(
        """\
        <table>
        <tr style="background-color: #eee">
            <th>Collaborator</th>
            <th colspan="2">Files</th>
            <th colspan="2">Size</th>
        </tr>
        """
    )

    new_time = current["*"]["start_time"]
    old_time = previous.get("*", {}).get("start_time", new_time)
    delta = datetime.timedelta(seconds=new_time - old_time)

    if delta:
        days = delta.days
        hours = delta.seconds // 3600
        message += f"<p>In the past {days} days and {hours} hours...</p>\n"
    else:
        message += textwrap.dedent(
            """\
            <p>
                This is the initial snapshot.
                Everything will be counted as "new".
            </p>
            """
        )

    message += "<h3>S3 buffer</h3>\n" + table_start

    total_f = 0
    total_b = 0
    total_df = 0
    total_db = 0
    i = 0

    for name in current:
        if name == "*":
            continue

        f = current[name]["s3_files"]
        b = current[name]["s3_bytes"]
        (h, si) = get_si_suffix(b)

        df = f - previous.get(name, {}).get("s3_files", 0)
        db = b - previous.get(name, {}).get("s3_bytes", 0)
        (dh, dsi) = get_si_suffix(db)

        total_f += f
        total_b += b
        total_df += df
        total_db += db

        i += 1
        if (i % 2) == 0:
            message += '<tr style="background-color: #dfd">'
        else:
            message += "<tr>"

        message += textwrap.dedent(
            f"""\
            <td style="{padding}">{name}</td>
            <td style="{padding}{num_style}">{f:,}</td>
            <td style="{padding}{num_style}">{df:+,}</td>
            <td style="{padding}{num_style}">{h:,.1f} {si}</td>
            <td style="{padding}{num_style}">{dh:+,.1f} {dsi}</td>
            </tr>
            """
        )

    (total_h, total_si) = get_si_suffix(total_b)
    (total_dh, total_dsi) = get_si_suffix(total_db)

    message += textwrap.dedent(
        f"""\
        <td style="{padding}"><b>Total</b></td>
        <td style="{padding}{num_style}">{total_f:,}</td>
        <td style="{padding}{num_style}">{total_df:+,}</td>
        <td style="{padding}{num_style}">{total_h:,.1f} {total_si}</td>
        <td style="{padding}{num_style}">{total_dh:+,.1f} {total_dsi}</td>
        </tr>
        </table>
        """
    )

    return (message, total_df, total_db)


def send_email(previous, current):
    """
    ???
    """
    # XXX

    now = current["*"]["start_time"]
    today = datetime.datetime.fromtimestamp(now).strftime("%Y-%m-%d")

    (html, total_df, total_db) = get_html_message(previous, current)
    (total_dh, total_dsi) = get_si_suffix(total_db)

    subject = (
        f"({total_df:,} files, {total_dh:+,.1f} {total_dsi}) "
        f"S3 Storage Report for {today}"
    )
    subject = subject.replace("&nbsp;", "")  # remove padding in `total_dsi`

    message = email.mime.multipart.MIMEMultipart("alternative")
    message["Subject"] = subject
    message["To"] = ",".join(TO)
    message.add_header("CC", ",".join(CC))
    message.add_header("Reply-To", REPLY_TO)

    part1 = email.mime.text.MIMEText(html, "html")
    plain = html2text.html2text(html)
    part2 = email.mime.text.MIMEText(plain, "plain")
    message.attach(part1)
    message.attach(part2)

    logging.debug(plain)

    context = ssl.create_default_context()
    server = smtplib.SMTP("smtp.wiscmail.wisc.edu")
    server.starttls(context=context)
    server.send_message(message, FROM, TO)
    server.quit()


def main() -> None:
    p = get_previous_snapshot()
    c = get_current_snapshot()

    save_snapshot(c)
    send_email(p, c)


if __name__ == "__main__":
    try:
        logging.basicConfig(
            format="%(asctime)s ~ %(message)s",
            level=logging.DEBUG,
        )
        main()
    except Exception as e:  # pylint: disable=broad-except
        logging.exception("Uncaught exception")
        sys.exit(1)
