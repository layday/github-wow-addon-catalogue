from __future__ import annotations

import argparse
import asyncio
import csv
import io
import os
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from itertools import count
from typing import Any
from zipfile import ZipFile

import aiohttp
from aiohttp_client_cache.backends.sqlite import SQLiteBackend
from aiohttp_client_cache.session import CachedSession
from loguru import logger
from yarl import URL

API_URL = URL("https://api.github.com/")

USER_AGENT = "github-wow-addon-catalogue (+https://github.com/layday/github-wow-addon-catalogue)"

Get = Callable[[str | URL], AbstractAsyncContextManager[aiohttp.ClientResponse]]


class ReleaseJsonFlavor(str, Enum):
    mainline = "mainline"
    classic = "classic"
    bcc = "bcc"


@dataclass(frozen=True)
class ProjectIds:
    curse_id: str | None
    wago_id: str | None
    wowi_id: str | None


@dataclass(frozen=True)
class Project:
    name: str
    description: str | None
    url: str
    last_updated: datetime
    flavors: frozenset[ReleaseJsonFlavor]
    ids: ProjectIds


def parse_toc_file(contents: str) -> dict[str, str]:
    return {
        k: v
        for e in contents.splitlines()
        if e.startswith("##")
        for k, v in ((i.strip() for i in e.lstrip("#").partition(":")[::2]),)
        if k
    }


async def find_release_json_repos(get: Get, queries: Sequence[str]):
    for query in queries:
        search_url = (API_URL / "search" / "code").with_query(q=query, per_page=100)
        while True:
            async with get(search_url) as response:
                content = await response.json()

            for i in content["items"]:
                yield i["repository"]

            next_url = response.links.get("next")
            if next_url is None:
                break
            search_url = next_url["url"]


async def extract_project_ids(
    get: Get, release: Mapping[str, Any], release_json: Mapping[str, Any]
):
    first_filename = release_json["releases"][0]["filename"]
    first_file_url = next(
        a["browser_download_url"] for a in release["assets"] if a["name"] == first_filename
    )
    async with get(first_file_url) as file_response:
        file = await file_response.read()

    with ZipFile(io.BytesIO(file)) as addon_zip:
        item_names = (
            n for n in addon_zip.namelist() if n.lower().endswith(".toc") and n.count("/") == 1
        )
        items = [addon_zip.read(n).decode() for n in item_names]

    if not items:
        return

    tocs = (parse_toc_file(c) for c in items)
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


async def parse_repo_has_release_json_releases(get: Get, repo: Mapping[str, Any]):
    async with get(
        (API_URL / "repos" / repo["full_name"] / "releases").with_query(per_page=1)
    ) as releases_response:
        releases = await releases_response.json()

    if releases:
        (release,) = releases
        release_json_asset = next(
            (a for a in release["assets"] if a["name"] == "release.json"), None
        )
        if release_json_asset is not None:
            async with get(release_json_asset["browser_download_url"]) as release_json_response:
                release_json = await release_json_response.json(content_type=None)

            project_ids = await extract_project_ids(get, release, release_json)
            if project_ids is None:
                project_ids = ProjectIds(*(None,) * 3)

            return Project(
                repo["name"],
                repo["description"],
                repo["html_url"],
                datetime.fromisoformat(f"{release['published_at'].rstrip('Z')}+00:00"),
                frozenset(
                    ReleaseJsonFlavor(m["flavor"])
                    for r in release_json["releases"]
                    for m in r["metadata"]
                ),
                project_ids,
            )


async def get_projects(token: str):
    async with CachedSession(
        cache=SQLiteBackend(
            "http-cache.db",
            urls_expire_after={
                f"{API_URL.host}/search/code": timedelta(days=1),
            },
        ),
        connector=aiohttp.TCPConnector(limit_per_host=3),
        headers={
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {token}",
            "User-Agent": USER_AGENT,
        },
        timeout=aiohttp.ClientTimeout(sock_connect=10, sock_read=10),
    ) as client:

        @asynccontextmanager
        async def get(url: str | URL):
            for attempt in count(1):
                if not client.cache.has_url(str(url)):
                    # Sophisticated secondary rate limiting prevention
                    await asyncio.sleep(1)

                async with client.get(url) as response:
                    logger.info(
                        f"fetched {response.url}, "
                        f"{response.headers.get('X-RateLimit-Remaining') or '?'} requests remaining"
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
            async for r in find_release_json_repos(
                get,
                [
                    "path:.github/workflows bigwigsmods packager",
                    "path:.github/workflows CF_API_KEY",
                ],
            )
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
    args = parser.parse_args()

    token = os.environ["RELEASE_JSON_ADDONS_GITHUB_TOKEN"]
    projects = asyncio.run(get_projects(token))

    with open(args.outcsv, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(
            (
                "name",
                "description",
                "url",
                "last_updated",
                "flavors",
                "curse_id",
                "wago_id",
                "wowi_id",
            )
        )
        csv_writer.writerows(
            (
                p.name,
                p.description,
                p.url,
                p.last_updated.isoformat(),
                ",".join(sorted(p.flavors)),
                p.ids.curse_id,
                p.ids.wago_id,
                p.ids.wowi_id,
            )
            for p in sorted(projects, key=lambda p: p.url)
        )


if __name__ == "__main__":
    main()
