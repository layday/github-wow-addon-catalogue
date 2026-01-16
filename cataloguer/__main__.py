from __future__ import annotations

import argparse
import asyncio
import csv
import datetime as dt
import logging
import os
import sys
from collections import defaultdict
from contextlib import contextmanager
from functools import partial
from typing import TYPE_CHECKING

import structlog

from . import PROJECT_FIELD_NAMES, Project, get_projects, get_projects_to_prune

if TYPE_CHECKING:
    from _typeshed import OpenTextMode


def main():
    make_parser = partial(
        argparse.ArgumentParser,
        prog=__spec__.parent,
        description="Harvester for World of Warcraft add-ons hosted in GitHub.",
    )
    if sys.version_info >= (3, 14):
        make_parser = partial(make_parser, suggest_on_error=True)

    main_parser = make_parser()
    main_parser.add_argument("-v", "--verbose", action="store_true", help="log more things")

    csv_parser = make_parser(add_help=False)
    csv_parser.add_argument("--outcsv", default="addons.csv", help="target CSV file")

    cmd_parsers = main_parser.add_subparsers(title="commands", required=True)

    collect_parser = cmd_parsers.add_parser("collect", parents=[csv_parser])
    collect_parser.add_argument(
        "--merge", action="store_true", help="merge with existing `OUTCSV`"
    )
    collect_parser.add_argument(
        "--search-user",
        action="append",
        dest="search_users",
        help="collect add-ons of a specific GitHub user",
    )
    collect_parser.set_defaults(func=_collect)

    prune_or_update_parser = cmd_parsers.add_parser("prune-or-update", parents=[csv_parser])
    prune_or_update_parser.add_argument(
        "--older-than", required=True, type=int, help="time delta in days"
    )
    prune_or_update_parser.set_defaults(func=_prune_or_update)

    find_duplicates_parser = cmd_parsers.add_parser("find-duplicates", parents=[csv_parser])
    find_duplicates_parser.set_defaults(func=_find_duplicates)

    args = main_parser.parse_args()
    if not args.verbose:
        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        )
    args.func(args)


@contextmanager
def _with_outcsv(outcsv: str, mode: OpenTextMode = "r"):
    with open(outcsv, mode, encoding="utf-8", newline="") as file:
        yield file


def _collect(args: argparse.Namespace):
    search_users: list[str] = args.search_users
    projects = asyncio.run(
        get_projects(
            token=os.environ["RELEASE_JSON_ADDONS_GITHUB_TOKEN"],
            search_params=tuple(("user_repositories", u) for u in search_users)
            if search_users
            else None,
        )
    )

    if args.merge:
        with _with_outcsv(args.outcsv) as outcsv_file:
            csv_reader = csv.DictReader(outcsv_file)
            rows = {r["id"]: r for r in csv_reader}
    else:
        rows = {}

    rows |= {(r := p.to_csv_row())["id"]: r for p in projects}

    with _with_outcsv(args.outcsv, "w") as outcsv_file:
        csv_writer = csv.DictWriter(outcsv_file, PROJECT_FIELD_NAMES)
        csv_writer.writeheader()
        csv_writer.writerows(sorted(rows.values(), key=lambda r: r["full_name"].lower()))


def _prune_or_update(args: argparse.Namespace):
    with _with_outcsv(args.outcsv) as outcsv_file:
        csv_reader = csv.DictReader(outcsv_file)
        projects = list(map(Project.from_csv_row, csv_reader))

    most_recently_harvested = max(p.last_seen for p in projects)
    cutoff = most_recently_harvested - dt.timedelta(days=args.older_than)

    repos_to_prune = asyncio.run(
        get_projects_to_prune(
            token=os.environ["RELEASE_JSON_ADDONS_GITHUB_TOKEN"],
            prune_candidates=(p.full_name for p in projects if p.last_seen < cutoff),
        )
    )

    with _with_outcsv(args.outcsv, "w") as outcsv_file:
        csv_writer = csv.DictWriter(outcsv_file, PROJECT_FIELD_NAMES)
        csv_writer.writeheader()
        csv_writer.writerows(
            i.to_csv_row()
            for p in projects
            for r in (repos_to_prune.get(p.full_name),)
            for i in (r if r is not None else p,)
            if i is not False
        )


def _find_duplicates(args: argparse.Namespace):
    with _with_outcsv(args.outcsv) as outcsv_file:
        csv_reader = csv.DictReader(outcsv_file)

        potential_dupes = defaultdict[tuple[str, str], list[str]](list)

        for project in csv_reader:
            for source, source_id_name in [
                ("curse", "curse_id"),
                ("wago", "wago_id"),
                ("wowi", "wowi_id"),
            ]:
                if project[source_id_name]:
                    potential_dupes[source, project[source_id_name]].append(project["full_name"])

        for (source, source_id), dupes in potential_dupes.items():
            if len(dupes) > 1:
                print(source, source_id, dupes)


if __name__ == "__main__":
    main()
