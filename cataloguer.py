from __future__ import annotations

import asyncio
import csv
import enum
import io
import json
import logging
import os
import re
import sqlite3
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager, contextmanager
from dataclasses import dataclass, fields
from datetime import UTC, datetime, timedelta
from itertools import chain, dropwhile, pairwise
from typing import Any, Literal, NewType, Protocol
from zipfile import ZipFile

import aiohttp
import click
import structlog
from aiohttp_client_cache import BaseCache, CacheBackend
from aiohttp_client_cache.cache_control import ExpirationPatterns
from aiohttp_client_cache.session import CachedSession
from cattrs import BaseValidationError, Converter
from cattrs.preconf.json import configure_converter as configure_json_converter
from yarl import URL

logger = structlog.get_logger()


@contextmanager
def get_sqlite_cache(db_path: str, cache: CacheBackend):
    with sqlite3.connect(db_path) as db_connection:
        db_connection.execute("PRAGMA journal_mode = wal")
        db_connection.execute("PRAGMA synchronous = normal")
        cache.responses = _SQLitePickleCache().prepare("responses", db_connection)
        cache.redirects = _SQLiteSimpleCache().prepare("redirects", db_connection)
        yield cache


class _SQLiteSimpleCache(BaseCache):
    @contextmanager
    def _transact(self):
        try:
            yield
        except BaseException:
            self._db_connection.rollback()
            raise
        else:
            self._db_connection.commit()

    def prepare(self, table_name: str, db_connection: sqlite3.Connection):
        self.table_name = table_name
        self._db_connection = db_connection
        self._db_connection.execute(
            f'CREATE TABLE IF NOT EXISTS "{self.table_name}" (key PRIMARY KEY, value)'
        )
        return self

    async def clear(self) -> None:
        raise NotImplementedError

    async def contains(self, key: str):
        cursor = self._db_connection.execute(
            f'SELECT 1 FROM "{self.table_name}" WHERE key = ?',
            (key,),
        )
        (value,) = cursor.fetchone() or (0,)
        return value

    async def delete(self, key: str) -> None:
        with self._transact():
            self._db_connection.execute(f'DELETE FROM "{self.table_name}" WHERE key = ?', (key,))

    async def bulk_delete(self, keys: object) -> None:
        raise NotImplementedError

    async def keys(self):
        cursor = self._db_connection.execute(f'SELECT key FROM "{self.table_name}"')
        for (value,) in cursor:
            yield value

    async def read(self, key: str):
        cursor = self._db_connection.execute(
            f'SELECT value FROM "{self.table_name}" WHERE key = ?',
            (key,),
        )
        (value,) = cursor.fetchone() or (None,)
        return value

    async def size(self):
        cursor = self._db_connection.execute(f'SELECT COUNT(key) FROM "{self.table_name}"')
        (value,) = cursor.fetchone()
        return value

    async def values(self):
        cursor = self._db_connection.execute(f'SELECT value FROM "{self.table_name}"')
        for (value,) in cursor:
            yield value

    async def write(self, key: str, item: Any):
        with self._transact():
            self._db_connection.execute(
                f'INSERT OR REPLACE INTO "{self.table_name}" (key, value) VALUES (?, ?)',
                (key, item),
            )


class _SQLitePickleCache(_SQLiteSimpleCache):
    async def read(self, key: str):
        return self.deserialize(await super().read(key))

    async def values(self):
        cursor = self._db_connection.execute(f'SELECT value FROM "{self.table_name}"')
        for (value,) in cursor:
            yield self.deserialize(value)

    async def write(self, key: str, item: Any):
        encoded_item = self.serialize(item)
        if encoded_item:
            await super().write(key, memoryview(encoded_item))


USER_AGENT = "github-wow-addon-catalogue (+https://github.com/layday/github-wow-addon-catalogue)"

API_URL = URL("https://api.github.com/")

_SEARCH_INTERVAL_HOURS = 2

CACHE_INDEFINITELY = -1
EXPIRE_URLS: ExpirationPatterns = {
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
    "alchem1ster/AddOns-Update-Tool",  # Not an add-on
    "alchem1ster/AddOnsFixer",  # Not an add-on
    "BilboTheGreedy/Azerite",  # Not an add-on
    "Centias/BankItems",  # Fork
    "DaMitchell/HelloWorld",  # Dummy add-on
    "dratr/BattlePetCount",  # Fork
    "gorilla-devs/",  # Minecraft stuff
    "HappyRot/AddOns",  # Compilation
    "hippuli/",  # Fork galore
    "JsMacros/",  # Minecraft stuff
    "juraj-hrivnak/Underdog",  # Minecraft stuff
    "Kirri777/WorldQuestsList",  # Fork
    "livepeer/",  # Minecraft stuff
    "lowlee/MikScrollingBattleText",  # Fork
    "lowlee/MSBTOptions",  # Fork
    "MikeD89/KarazhanChess",  # Hijacking BigWigs' TOC IDs, probably by accident
    "Oppzippy/HuokanGoldLogger",  # Archived
    "pinged-eu/wow-addon-helloworld",  # Dummy add-on
    "rePublic-Studios/rPLauncher",  # Minecraft stuff
    "smashedr/MethodAltManager",  # Fork
    "unix/curseforge-release",  # Template
    "wagyourtail/JsMacros",  # More Minecraft stuff
    "ynazar1/Arh",  # Fork
)


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
    rf"""
        ^
        (?P<name>[^/]+)
        [/]
        (?P=name)
        (?:[-_](?P<flavor>{'|'.join(map(re.escape, TOC_ALIASES))}))?
        \.toc
        $
    """,
    flags=re.I | re.X,
)

INTERFACE_RANGES_TO_FLAVORS = {
    range(1_00_00, 1_13_00): ReleaseJsonFlavor.mainline,
    range(1_13_00, 2_00_00): ReleaseJsonFlavor.classic,
    range(2_00_00, 2_05_00): ReleaseJsonFlavor.mainline,
    range(2_05_00, 3_00_00): ReleaseJsonFlavor.bcc,
    range(3_00_00, 3_04_00): ReleaseJsonFlavor.mainline,
    range(3_04_00, 4_00_00): ReleaseJsonFlavor.wrath,
    range(4_00_00, 11_00_00): ReleaseJsonFlavor.mainline,
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


def interface_numbers_to_flavours(interface_numbers: Iterable[int]):
    for interface_number in interface_numbers:
        for interface_range, flavor in INTERFACE_RANGES_TO_FLAVORS.items():
            if interface_number in interface_range:
                yield flavor
                break


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


async def extract_project_ids_from_toc_files(get: Get, url: str, filename: str):
    async with get(url) as file_response:
        file = await file_response.read()

    with ZipFile(io.BytesIO(file)) as addon_zip:
        toc_file_contents = [
            addon_zip.read(n).decode("utf-8-sig")
            for n in addon_zip.namelist()
            for m in (TOP_LEVEL_TOC_NAME_PATTERN.match(n),)
            if m and filename.startswith(m["name"].lstrip("!_"))
        ]

    if toc_file_contents:
        tocs = (parse_toc_file(c) for c in toc_file_contents)
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
        logger.warning(f"unable to find conformant TOC files in {url}")
        project_ids = (None,) * 3

    return dict(zip(("curse_id", "wago_id", "wowi_id"), project_ids))


async def extract_game_flavors_from_tocs(get: Get, release_archives: Sequence[dict[str, Any]]):
    for release_archive in release_archives:
        async with get(release_archive["browser_download_url"]) as file_response:
            file = await file_response.read()

        with ZipFile(io.BytesIO(file)) as addon_zip:
            toc_names = [
                (n, TOC_ALIASES.get(m["flavor"]))
                for n in addon_zip.namelist()
                for m in (TOP_LEVEL_TOC_NAME_PATTERN.match(n),)
                if m
            ]
            yield (f for _, f in toc_names if f is not None)

            flavorless_toc_names = [n for n, f in toc_names if f is None]
            if flavorless_toc_names:
                tocs = (
                    parse_toc_file(addon_zip.read(n).decode("utf-8-sig"))
                    for n in flavorless_toc_names
                )
                interface_numbers = (
                    int(i) for t in tocs for i in (t.get("Interface"),) if i and i.isdigit()
                )
                yield interface_numbers_to_flavours(interface_numbers)


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
                        "release.json is not valid JSON:"
                        f" {maybe_release_json_asset['browser_download_url']}"
                    )
                    return

            try:
                release_json = ReleaseJson.from_dict(release_json_contents)
            except BaseValidationError:
                logger.exception(
                    "release.json has incorrect schema:"
                    f" {maybe_release_json_asset['browser_download_url']}"
                )
                return

            flavors = frozenset(m.flavor for r in release_json.releases for m in r.metadata)
            release_json_release = release_json.releases[0]
            try:
                project_ids = await extract_project_ids_from_toc_files(
                    get,
                    next(
                        a["browser_download_url"]
                        for a in release["assets"]
                        if a["name"] == release_json_release.filename
                    ),
                    release_json_release.filename,
                )
            except StopIteration:
                logger.warning(f"release asset does not exist: {release_json_release.filename}")
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
                {f async for a in extract_game_flavors_from_tocs(get, release_archives) for f in a}
            )
            release_archive = release_archives[0]
            project_ids = await extract_project_ids_from_toc_files(
                get, release_archive["browser_download_url"], release_archive["name"]
            )

        return Project(
            name=repo["name"],
            full_name=repo["full_name"],
            url=repo["html_url"],
            description=repo["description"],
            last_updated=datetime.fromisoformat(f"{release['published_at'].rstrip('Z')}+00:00"),
            flavors=ProjectFlavors(flavors),
            has_release_json=maybe_release_json_asset is not None,
            last_seen=datetime.now(UTC),
            **project_ids,
        )


async def get_projects(token: str):
    with get_sqlite_cache(
        "http-cache.db",
        CacheBackend(
            expire_after=CACHE_INDEFINITELY,
            urls_expire_after=EXPIRE_URLS,
        ),
    ) as cache:
        async with CachedSession(
            cache=cache,
            connector=aiohttp.TCPConnector(limit_per_host=8),
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"token {token}",
                "User-Agent": USER_AGENT,
            },
            timeout=aiohttp.ClientTimeout(sock_connect=10, sock_read=10),
        ) as client:

            @asynccontextmanager
            async def get(url: str | URL):
                for _ in range(5):
                    async with client.get(url) as response:
                        logger.debug(
                            f"fetching {response.url}"
                            + (
                                f"\n\t{response.headers['X-RateLimit-Remaining'] or '?'} requests"
                                " remaining"
                                if "X-RateLimit-Remaining" in response.headers
                                else ""
                            )
                        )

                        if response.status == 403:
                            logger.error(await response.text())

                            sleep_until_header = response.headers.get("X-RateLimit-Reset")
                            if sleep_until_header:
                                sleep_until = datetime.fromtimestamp(  # noqa: DTZ006
                                    int(response.headers["X-RateLimit-Reset"])
                                )
                                sleep_for = max(
                                    0,
                                    (sleep_until - datetime.now()).total_seconds(),  # noqa: DTZ005
                                )
                                logger.info(
                                    "rate limited after"
                                    f" {response.headers['X-RateLimit-Used']} requests, sleeping"
                                    f" until {sleep_until.time().isoformat()} for"
                                    f" ({sleep_for} + 1)s"
                                )
                                await asyncio.sleep(sleep_for + 1)

                        elif not response.ok:
                            logger.debug(
                                f"request errored with status {response.status}; sleeping for 3s"
                            )
                            await asyncio.sleep(3)
                            continue

                        else:
                            yield response
                            break
                else:
                    response.raise_for_status()  # pyright: ignore[reportUnboundVariable]

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

        now = datetime.now(UTC)
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
    if not verbose:
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
