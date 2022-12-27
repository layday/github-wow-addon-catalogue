from __future__ import annotations

import asyncio
import csv
import enum
import io
import json
import logging
import os
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, fields
from datetime import datetime, timedelta, timezone
from itertools import chain, dropwhile, pairwise
from typing import Any, Literal, NewType, Protocol
from zipfile import ZipFile

import aiohttp
import click
import structlog
from aiohttp_client_cache.backends.sqlite import SQLiteBackend
from aiohttp_client_cache.session import CachedSession
from cattrs import BaseValidationError, Converter
from cattrs.preconf.json import configure_converter as configure_json_converter
from yarl import URL

logger = structlog.get_logger()


USER_AGENT = "github-wow-addon-catalogue (+https://github.com/layday/github-wow-addon-catalogue)"

API_URL = URL("https://api.github.com/")

_SEARCH_INTERVAL_HOURS = 2

CACHE_INDEFINITELY = -1
EXPIRE_URLS = {
    f"{API_URL.host}/search": timedelta(hours=_SEARCH_INTERVAL_HOURS),
    f"{API_URL.host}/repos/*/releases": timedelta(days=1),
}

MIN_PRUNE_RUNS = 5
MIN_PRUNE_INTERVAL = timedelta(hours=_SEARCH_INTERVAL_HOURS)
PRUNE_CUTOFF = timedelta(hours=_SEARCH_INTERVAL_HOURS * MIN_PRUNE_RUNS)

OTHER_SOURCES = [
    ("curse", "curse_id"),
    ("wago", "wago_id"),
    ("wowi", "wowi_id"),
]


class Get(Protocol):
    def __call__(self, url: str | URL) -> AbstractAsyncContextManager[aiohttp.ClientResponse]:
        ...


REPO_EXCLUDES = (
    "alchem1ster/AddOns-Update-Tool",
    "BilboTheGreedy/Azerite",
    "Centias/BankItems",
    "DaMitchell/HelloWorld",
    "dratr/BattlePetCount",
    "HappyRot/AddOns",
    "hippuli/",
    "JsMacros/",
    "Kirri777/WorldQuestsList",
    "livepeer/",
    "lowlee/MikScrollingBattleText",
    "lowlee/MSBTOptions",
    "MikeD89/KarazhanChess",
    "smashedr/MethodAltManager",
    "wagyourtail/JsMacros",
    "ynazar1/Arh",
)


@enum.global_enum
class _UnkFlavor(enum.Enum):
    UNK_FLAVOR = "UNK_FLAVOR"


UNK_FLAVOR = _UnkFlavor.UNK_FLAVOR


class ReleaseJsonFlavor(enum.StrEnum):
    mainline = "mainline"
    classic = "classic"
    bcc = "bcc"
    wrath = "wrath"


@dataclass
class ReleaseJson:
    releases: list[ReleaseJsonRelease]

    @classmethod
    def from_dict(cls, values: object):
        return _release_json_converter.structure(values, cls)


@dataclass
class ReleaseJsonRelease:
    filename: str
    nolib: bool
    metadata: list[ReleaseJsonReleaseMetadata]


@dataclass
class ReleaseJsonReleaseMetadata:
    flavor: ReleaseJsonFlavor
    interface: int


_release_json_converter = Converter()


TOC_ALIASES = {
    **ReleaseJsonFlavor.__members__,
    "vanilla": ReleaseJsonFlavor.classic,
    "tbc": ReleaseJsonFlavor.bcc,
    "wotlkc": ReleaseJsonFlavor.wrath,
}

TOP_LEVEL_TOC_NAME_PATTERN = re.compile(
    rf"^(?P<name>[^/]+)[/](?P=name)(?:[-_](?P<flavor>{'|'.join(map(re.escape, TOC_ALIASES))}))?\.toc$",
    flags=re.I,
)

FLAVORS_TO_INTERFACE_RANGES = {
    ReleaseJsonFlavor.mainline: (
        range(1_00_00, 1_13_00),
        range(2_00_00, 2_05_00),
        range(3_00_00, 3_04_00),
        range(4_00_00, 11_00_00),
    ),
    ReleaseJsonFlavor.classic: (range(1_13_00, 2_00_00),),
    ReleaseJsonFlavor.bcc: (range(2_05_00, 3_00_00),),
    ReleaseJsonFlavor.wrath: (range(3_04_00, 4_00_00),),
}


ProjectFlavors = NewType("ProjectFlavors", frozenset[ReleaseJsonFlavor])


@dataclass(frozen=True, kw_only=True)
class Project:
    name: str
    full_name: str
    url: str
    description: str | None
    last_updated: datetime
    flavors: ProjectFlavors
    curse_id: str | None
    wago_id: str | None
    wowi_id: str | None
    has_release_json: bool
    last_seen: datetime

    @classmethod
    def from_csv_row(cls, values: object):
        return _addons_csv_converter.structure(values, cls)

    def to_csv_row(self):
        return _addons_csv_converter.unstructure(self)


project_field_names = tuple(f.name for f in fields(Project))

_addons_csv_converter = Converter()
configure_json_converter(_addons_csv_converter)
_addons_csv_converter.register_unstructure_hook(ProjectFlavors, lambda v: ",".join(sorted(v)))
_addons_csv_converter.register_structure_hook(
    bool,
    lambda v, _: v == "True",
)
_addons_csv_converter.register_structure_hook(
    ProjectFlavors,
    lambda v, _: [ReleaseJsonFlavor(f) for f in v.split(",") if f],
)


def parse_toc_file(contents: str):
    return {
        k: v
        for e in contents.splitlines()
        if e.startswith("##")
        for k, v in ((i.strip() for i in e.lstrip("#").partition(":")[::2]),)
        if k
    }


def is_top_level_addon_toc(zip_path: str):
    match = TOP_LEVEL_TOC_NAME_PATTERN.match(zip_path)
    if match:
        return TOC_ALIASES.get(match["flavor"], UNK_FLAVOR)


async def find_addon_repos(
    get: Get,
    queries_by_endpoint: Sequence[
        tuple[Literal["code", "repositories", "user_repositories"], str]
    ],
):
    for endpoint, query in queries_by_endpoint:
        if endpoint == "user_repositories":
            async with get(API_URL / "users" / query / "repos") as response:
                repos = await response.json()

            for repo in repos:
                yield repo

        else:
            search_url = (API_URL / "search" / endpoint).with_query(q=query, per_page=100)
            while True:
                async with get(search_url) as response:
                    content = await response.json()

                for item in content["items"]:
                    yield (item["repository"] if endpoint == "code" else item)

                next_url = response.links.get("next")
                if next_url is None:
                    break
                search_url = next_url["url"]


async def extract_project_ids_from_toc_files(get: Get, url: str):
    async with get(url) as file_response:
        file = await file_response.read()

    with ZipFile(io.BytesIO(file)) as addon_zip:
        unparsed_tocs = [
            addon_zip.read(n).decode("utf-8-sig")
            for n in addon_zip.namelist()
            if is_top_level_addon_toc(n)
        ]

    if unparsed_tocs:
        tocs = (parse_toc_file(c) for c in unparsed_tocs)
        toc_ids = (
            (
                t.get("X-Curse-Project-ID"),
                t.get("X-Wago-ID"),
                t.get("X-WoWI-ID"),
            )
            for t in tocs
        )
        project_ids = (next(filter(None, s), None) for s in zip(*toc_ids))

    else:
        project_ids = (None,) * 3

    return dict(zip(("curse_id", "wago_id", "wowi_id"), project_ids))


async def extract_game_flavors_from_toc_files(
    get: Get, release_archives: Sequence[dict[str, Any]]
):
    for release_archive in release_archives:
        async with get(release_archive["browser_download_url"]) as file_response:
            file = await file_response.read()

        with ZipFile(io.BytesIO(file)) as addon_zip:
            toc_names = [
                (n, f) for n in addon_zip.namelist() for f in (is_top_level_addon_toc(n),) if f
            ]
            flavors_from_toc_names = {f for _, f in toc_names if f is not UNK_FLAVOR}
            if flavors_from_toc_names:
                yield flavors_from_toc_names

            unk_flavor_toc_names = [n for n, f in toc_names if f is UNK_FLAVOR]
            if unk_flavor_toc_names:
                tocs = (
                    parse_toc_file(addon_zip.read(n).decode("utf-8-sig"))
                    for n in unk_flavor_toc_names
                )
                interface_versions = (
                    int(i) for t in tocs for i in (t.get("Interface"),) if i and i.isdigit()
                )
                flavors_from_interface_versions = {
                    f
                    for v in interface_versions
                    for f, r in FLAVORS_TO_INTERFACE_RANGES.items()
                    if any(v in i for i in r)
                }
                yield flavors_from_interface_versions


async def parse_repo(get: Get, repo: Mapping[str, Any]):
    async with get(
        (API_URL / "repos" / repo["full_name"] / "releases").with_query(per_page=1)
    ) as releases_response:
        releases = await releases_response.json()

    if releases:
        (release,) = releases
        maybe_release_json_asset = next(
            (a for a in release["assets"] if a["name"] == "release.json"), None
        )
        if maybe_release_json_asset is not None:
            async with get(
                maybe_release_json_asset["browser_download_url"]
            ) as release_json_response:
                try:
                    release_json_contents = await release_json_response.json(content_type=None)
                except json.JSONDecodeError:
                    logger.exception(
                        f"release.json is not valid JSON: {maybe_release_json_asset['browser_download_url']}"
                    )
                    return

            try:
                release_json = ReleaseJson.from_dict(release_json_contents)
            except BaseValidationError:
                logger.exception(
                    f"release.json has incorrect schema: {maybe_release_json_asset['browser_download_url']}"
                )
                return

            flavors = frozenset(m.flavor for r in release_json.releases for m in r.metadata)
            try:
                project_ids = await extract_project_ids_from_toc_files(
                    get,
                    next(
                        a["browser_download_url"]
                        for a in release["assets"]
                        if a["name"] == release_json.releases[0].filename
                    ),
                )
            except StopIteration:
                logger.warning(
                    f"release asset does not exist: {release_json.releases[0].filename}"
                )
                return

        else:
            release_archives = [
                a
                for a in release["assets"]
                if a["content_type"] in {"application/zip", "application/x-zip-compressed"}
                and a["name"].endswith(".zip")
            ]
            if not release_archives:
                # Not a downloadable add-on
                return

            flavors = frozenset(
                {
                    f
                    async for a in extract_game_flavors_from_toc_files(get, release_archives)
                    for f in a
                }
            )
            project_ids = await extract_project_ids_from_toc_files(
                get, release_archives[0]["browser_download_url"]
            )

        return Project(
            name=repo["name"],
            full_name=repo["full_name"],
            url=repo["html_url"],
            description=repo["description"],
            last_updated=datetime.fromisoformat(f"{release['published_at'].rstrip('Z')}+00:00"),
            flavors=ProjectFlavors(flavors),
            has_release_json=maybe_release_json_asset is not None,
            last_seen=datetime.now(timezone.utc),
            **project_ids,
        )


async def get_projects(token: str):
    async with CachedSession(
        cache=SQLiteBackend(
            "http-cache.db",
            expire_after=CACHE_INDEFINITELY,
            urls_expire_after=EXPIRE_URLS,
        ),
        connector=aiohttp.TCPConnector(limit_per_host=8),
        headers={
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {token}",
            "User-Agent": USER_AGENT,
        },
        timeout=aiohttp.ClientTimeout(sock_connect=10, sock_read=10),
    ) as client:
        # aiohttp-client-cache opens a new connection for every request
        # and locks up the db, we'll limit it to 10 concurrent connections
        # for now
        db_semaphore = asyncio.Semaphore(10)

        @asynccontextmanager
        async def get(url: str | URL):
            response = None
            for _ in range(5):
                async with db_semaphore, client.get(url) as response:
                    logger.debug(
                        f"fetching {response.url}"
                        f"\n\t{response.headers.get('X-RateLimit-Remaining') or '?'} requests remaining"
                    )

                    if response.status == 403:
                        logger.error(await response.text())

                        sleep_until_header = response.headers.get("X-RateLimit-Reset")
                        if sleep_until_header:
                            sleep_until = datetime.fromtimestamp(
                                int(response.headers["X-RateLimit-Reset"])
                            )
                            sleep_for = max(0, (sleep_until - datetime.now()).total_seconds())
                            logger.info(
                                f"rate limited after {response.headers['X-RateLimit-Used']} requests, "
                                f"sleeping until {sleep_until.time().isoformat()} for ({sleep_for} + 1)s"
                            )
                            await asyncio.sleep(sleep_for + 1)

                    elif not response.ok:
                        continue

                    else:
                        yield response
                        break
            else:
                if response:
                    response.raise_for_status()

        deduped_repos = {
            r["full_name"]: r
            async for r in find_addon_repos(
                get,
                [
                    ("code", "path:.github/workflows bigwigsmods packager"),
                    ("code", "path:.github/workflows CF_API_KEY"),
                    ("repositories", "topic:wow-addon"),
                    ("repositories", "topics:>2 topic:world-of-warcraft topic:addon"),
                ],
            )
            if not r["full_name"].startswith(REPO_EXCLUDES)
        }
        projects = {
            r
            for c in asyncio.as_completed([parse_repo(get, r) for r in deduped_repos.values()])
            for r in (await c,)
            if r
        }
        return projects


def log_run():
    with open("runs.json", "a+", encoding="utf-8") as runs_json:
        runs_json.seek(0)

        try:
            orig_runs = [datetime.fromisoformat(i) for i in json.load(runs_json)]
        except json.JSONDecodeError:
            orig_runs = []

        now = datetime.now(timezone.utc)
        combined_runs = set(
            chain(
                orig_runs[-MIN_PRUNE_RUNS:],
                dropwhile(lambda i: (now - i) > PRUNE_CUTOFF, orig_runs),
                (now,),
            )
        )

        runs_json.truncate(0)
        json.dump([i.isoformat() for i in sorted(combined_runs)], runs_json, indent=2)


def validate_runs(runs: list[str]):
    if len(runs) < MIN_PRUNE_RUNS:
        raise ValueError(
            f"cannot prune add-ons if fewer than {MIN_PRUNE_RUNS} runs have been logged"
        )
    elif (
        sum((b - a) >= MIN_PRUNE_INTERVAL for a, b in pairwise(map(datetime.fromisoformat, runs)))
        < MIN_PRUNE_RUNS
    ):
        raise ValueError(
            f"cannot prune add-ons if fewer than {MIN_PRUNE_RUNS} runs "
            f"which are {MIN_PRUNE_INTERVAL} apart have been logged"
        )


outcsv_argument = click.argument("outcsv", default="addons.csv")


@click.group(context_settings={"help_option_names": ("-h", "--help")})
@click.option("--verbose", "-v", is_flag=True, help="log more things")
def cli(verbose: bool):
    if verbose:
        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.INFO))


@cli.command
@outcsv_argument
@click.option("--merge", is_flag=True, help="merge with existing `OUTCSV`")
def collect(outcsv: str, merge: bool):
    token = os.environ["RELEASE_JSON_ADDONS_GITHUB_TOKEN"]
    projects = asyncio.run(get_projects(token))

    rows = {p.full_name.lower(): p.to_csv_row() for p in projects}

    if merge:
        with open(outcsv, encoding="utf-8", newline="") as addons_csv:
            csv_reader = csv.DictReader(addons_csv)
            rows = {r["full_name"].lower(): r for r in csv_reader} | rows

    with open(outcsv, "w", encoding="utf-8", newline="") as addons_csv:
        csv_writer = csv.DictWriter(addons_csv, project_field_names)
        csv_writer.writeheader()
        csv_writer.writerows(r for _, r in sorted(rows.items(), key=lambda r: r[0].lower()))

    log_run()


@cli.command
@outcsv_argument
@click.option("--older-than", required=True, type=int, help="time delta in days")
def prune(outcsv: str, older_than: int):
    with open("runs.json", encoding="utf-8") as runs_json:
        runs = json.load(runs_json)
        validate_runs(runs)

    with open(outcsv, encoding="utf-8", newline="") as addons_csv:
        csv_reader = csv.DictReader(addons_csv)
        projects = [Project.from_csv_row(r) for r in csv_reader if r["last_seen"]]

    most_recently_harvested = max(p.last_seen for p in projects)
    cutoff = most_recently_harvested - timedelta(days=older_than)

    with open(outcsv, "w", encoding="utf-8", newline="") as addons_csv:
        csv_writer = csv.DictWriter(addons_csv, project_field_names)
        csv_writer.writeheader()
        csv_writer.writerows(p.to_csv_row() for p in projects if p.last_seen >= cutoff)


@cli.command
@outcsv_argument
def find_duplicates(outcsv: str):
    with open(outcsv, encoding="utf-8", newline="") as addons_csv:
        csv_reader = csv.DictReader(addons_csv)

        potential_dupes = defaultdict[tuple[str, str], list[str]](list)

        for project in csv_reader:
            for source, source_id_name in OTHER_SOURCES:
                if project[source_id_name]:
                    potential_dupes[source, project[source_id_name]].append(project["full_name"])

        for (source, source_id), dupes in potential_dupes.items():
            if len(dupes) > 1:
                print(source, source_id, dupes)


if __name__ == "__main__":
    cli()