from __future__ import annotations

import argparse
import asyncio
import csv
import io
import os
import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from itertools import count
from typing import Any, Literal
from zipfile import ZipFile

import aiohttp
from aiohttp_client_cache.backends.sqlite import SQLiteBackend
from aiohttp_client_cache.session import CachedSession
from loguru import logger
from yarl import URL

API_URL = URL("https://api.github.com/")

USER_AGENT = "github-wow-addon-catalogue (+https://github.com/layday/github-wow-addon-catalogue)"

Get = Callable[["str | URL"], AbstractAsyncContextManager[aiohttp.ClientResponse]]


EXCLUDES = (
    "alchem1ster/AddOns-Update-Tool",
    "BilboTheGreedy/Azerite",
    "DaMitchell/HelloWorld",
    "HappyRot/AddOns",
    "hippuli/",
    "JsMacros/",
    "livepeer/",
    "mdd3/WCLRanks-Firemaw-EU-Alliance",
    "wagyourtail/JsMacros",
)


class ReleaseJsonFlavor(str, Enum):
    mainline = "mainline"
    classic = "classic"
    bcc = "bcc"
    wrath = "wrath"

    # TOC aliases
    vanilla = classic
    tbc = bcc


class _UnknownFlavor(Enum):
    UNK = "UNK"


UNK = _UnknownFlavor.UNK

match_top_level_toc_name = re.compile(
    r"^(?P<name>[^\/]+)[\/](?P=name)(?:[-|_]"
    rf"(?P<flavor>{'|'.join(map(re.escape, ReleaseJsonFlavor.__members__))}))?"
    r"\.toc$",
    flags=re.I,
)


FLAVORS_TO_INTERFACE_RANGES = {
    ReleaseJsonFlavor.mainline: (
        range(1_00_00, 1_13_00), range(2_00_00, 2_05_00), range(3_00_00, 3_04_00), range(4_00_00, 11_00_00)  # fmt: skip
    ),
    ReleaseJsonFlavor.classic: (range(1_13_00, 2_00_00),),
    ReleaseJsonFlavor.bcc: (range(2_05_00, 3_00_00),),
    ReleaseJsonFlavor.wrath: (range(3_04_00, 4_00_00),),
}


@dataclass(frozen=True)
class ProjectIds:
    curse_id: str | None
    wago_id: str | None
    wowi_id: str | None


@dataclass(frozen=True)
class Project:
    name: str
    full_name: str
    url: str
    description: str | None
    last_updated: datetime
    flavors: frozenset[ReleaseJsonFlavor]
    ids: ProjectIds
    has_release_json: bool


def parse_toc_file(contents: str):
    return {
        k: v
        for e in contents.splitlines()
        if e.startswith("##")
        for k, v in ((i.strip() for i in e.lstrip("#").partition(":")[::2]),)
        if k
    }


def is_top_level_addon_toc(zip_path: str):
    match = match_top_level_toc_name.match(zip_path)
    if match:
        return ReleaseJsonFlavor.__members__.get(match["flavor"], UNK)


async def find_addon_repos(
    get: Get, queries_by_kind: Sequence[tuple[Literal["code", "repositories"], str]]
):
    for kind, query in queries_by_kind:
        search_url = (API_URL / "search" / kind).with_query(q=query, per_page=100)
        while True:
            async with get(search_url) as response:
                content = await response.json()

            for i in content["items"]:
                yield (i["repository"] if kind == "code" else i)

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

    if not unparsed_tocs:
        return

    tocs = (parse_toc_file(c) for c in unparsed_tocs)
    toc_ids = (
        (
            t.get("X-Curse-Project-ID"),
            t.get("X-Wago-ID"),
            t.get("X-WoWI-ID"),
        )
        for t in tocs
    )
    project_ids = ProjectIds(*(next(filter(None, s), None) for s in zip(*toc_ids)))
    logger.info(f"extracted {project_ids}")
    return project_ids


async def extract_game_flavours_from_toc_files(get: Get, release_archives: Sequence[Any]):
    for release_archive in release_archives:
        async with get(release_archive["browser_download_url"]) as file_response:
            file = await file_response.read()

        with ZipFile(io.BytesIO(file)) as addon_zip:
            toc_names = [
                (n, f) for n in addon_zip.namelist() for f in (is_top_level_addon_toc(n),) if f
            ]
            flavours_from_toc_names = {f for _, f in toc_names if f is not UNK}
            if flavours_from_toc_names:
                yield flavours_from_toc_names

            unk_flavor_toc_names = [n for n, f in toc_names if f is UNK]
            if unk_flavor_toc_names:
                tocs = (
                    parse_toc_file(addon_zip.read(n).decode("utf-8-sig"))
                    for n in unk_flavor_toc_names
                )
                interface_versions = (
                    int(i) for t in tocs for i in (t.get("Interface"),) if i and i.isdigit()
                )
                flavours_from_interface_versions = {
                    f
                    for v in interface_versions
                    for f, r in FLAVORS_TO_INTERFACE_RANGES.items()
                    if any(v in i for i in r)
                }
                yield flavours_from_interface_versions


async def parse_repo_has_release_json_releases(get: Get, repo: Mapping[str, Any]):
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
                release_json = await release_json_response.json(content_type=None)

            flavours = frozenset(
                ReleaseJsonFlavor(m["flavor"])
                for r in release_json["releases"]
                for m in r["metadata"]
            )
            project_ids = await extract_project_ids_from_toc_files(
                get,
                next(
                    a["browser_download_url"]
                    for a in release["assets"]
                    if a["name"] == release_json["releases"][0]["filename"]
                ),
            )

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

            flavours = frozenset(
                {
                    f
                    async for a in extract_game_flavours_from_toc_files(get, release_archives)
                    for f in a
                }
            )
            project_ids = await extract_project_ids_from_toc_files(
                get, release_archives[0]["browser_download_url"]
            )

        return Project(
            repo["name"],
            repo["full_name"],
            repo["html_url"],
            repo["description"],
            datetime.fromisoformat(f"{release['published_at'].rstrip('Z')}+00:00"),
            flavours,
            project_ids or ProjectIds(None, None, None),
            maybe_release_json_asset is not None,
        )


async def get_projects(token: str):
    async with CachedSession(
        cache=SQLiteBackend(
            "http-cache.db",
            urls_expire_after={
                f"{API_URL.host}/search": timedelta(hours=2),
                f"{API_URL.host}/repos/*/releases": timedelta(days=1),
            },
        ),
        connector=aiohttp.TCPConnector(limit_per_host=8),
        headers={
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {token}",
            "User-Agent": USER_AGENT,
        },
        timeout=aiohttp.ClientTimeout(sock_connect=10, sock_read=10),
    ) as client:

        search_lock = asyncio.Lock()

        @asynccontextmanager
        async def rate_limit():
            async with search_lock:
                yield
                await asyncio.sleep(5)

        @asynccontextmanager
        async def noop():
            yield

        @asynccontextmanager
        async def get(url: str | URL):
            for attempt in count(1):
                async with (
                    rate_limit() if URL(url).path.startswith("/search") else noop()
                ), client.get(url) as response:
                    logger.info(
                        f"fetched {response.url}"
                        f"\n\t{response.headers.get('X-RateLimit-Remaining') or '?'} requests remaining"
                    )
                    if response.status == 403:
                        logger.error(await response.text())
                        sleep_until = datetime.fromtimestamp(
                            int(response.headers["X-RateLimit-Reset"])
                        )
                        sleep_for = max(0, (sleep_until - datetime.now()).total_seconds())
                        if sleep_for:
                            logger.info(
                                f"rate limited after {response.headers['X-RateLimit-Used']} requests, "
                                f"sleeping until {sleep_until.time().isoformat()} for {sleep_for}s"
                            )
                            await asyncio.sleep(sleep_for)
                    else:
                        if attempt == 3:
                            response.raise_for_status()
                        elif not response.ok:
                            continue

                        yield response
                        break

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
            if not r["full_name"].startswith(EXCLUDES)
        }
        projects = {
            r
            for c in asyncio.as_completed(
                [parse_repo_has_release_json_releases(get, r) for r in deduped_repos.values()]
            )
            for r in (await c,)
            if r
        }
        return projects


@logger.catch
def main():
    parser = argparse.ArgumentParser(description="Collect WoW add-on metadata from GitHub")
    parser.add_argument("outcsv", nargs="?", default="addons.csv")
    parser.add_argument("--merge", action="store_true")
    args = parser.parse_args()

    token = os.environ["RELEASE_JSON_ADDONS_GITHUB_TOKEN"]
    projects = asyncio.run(get_projects(token))

    rows = {
        p.url: (
            p.name,
            p.full_name,
            p.url,
            p.description,
            p.last_updated.isoformat(),
            ",".join(sorted(p.flavors)),
            p.ids.curse_id,
            p.ids.wago_id,
            p.ids.wowi_id,
            p.has_release_json,
        )
        for p in projects
    }

    if args.merge:
        with open(args.outcsv, "r", encoding="utf-8", newline="") as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip header.
            rows = {p[2]: p for p in csv_reader} | rows

    with open(args.outcsv, "w", encoding="utf-8", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(
            (
                "name",
                "full_name",
                "url",
                "description",
                "last_updated",
                "flavors",
                "curse_id",
                "wago_id",
                "wowi_id",
                "has_release_json",
            )
        )
        csv_writer.writerows(r for _, r in sorted(rows.items(), key=lambda r: r[0].lower()))


if __name__ == "__main__":
    main()
