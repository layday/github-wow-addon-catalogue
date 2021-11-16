from __future__ import annotations

import asyncio
import csv
import os
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import partial
from typing import Any

import httpx
from loguru import logger


class ReleaseJsonFlavor(str, Enum):
    mainline = "mainline"
    classic = "classic"
    bcc = "bcc"


@dataclass(frozen=True)
class Project:
    name: str
    description: str | None
    url: str
    last_updated: datetime
    flavors: frozenset[ReleaseJsonFlavor]


async def get_rate_limited(get_coro_fn: Callable[..., Awaitable[httpx.Response]], count: int = 0):
    try:
        response = await get_coro_fn()
    except ValueError:
        return await get_rate_limited(get_coro_fn)  # Totally not an infinite loop.

    logger.info(f"fetched {response.url}")
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        # We might be rate limited but the headers say we aren't, so uh...
        # just retry a couple of times then give up, I guess?
        if count > 1:
            raise

        logger.exception("rate limited? sleeping for 60s")
        await asyncio.sleep(60)
        return await get_rate_limited(get_coro_fn, count + 1)
    else:
        return response


async def find_release_json_repos(client: httpx.AsyncClient, queries: list[str]):
    for response_coro in (
        partial(client.get, "search/code", params={"q": q, "per_page": 100}) for q in queries
    ):
        while True:
            response = await get_rate_limited(response_coro)
            content = response.json()
            yield [i["repository"] for i in content["items"]]

            next_url = response.links.get("next")
            if next_url is None:
                break
            response_coro = partial(client.get, next_url["url"])


async def parse_repo_has_release_json_releases(client: httpx.AsyncClient, repo: Mapping[str, Any]):
    releases_response = await get_rate_limited(
        partial(client.get, f"repos/{repo['full_name']}/releases", params={"per_page": 1})
    )
    releases = releases_response.json()
    if releases:
        (release,) = releases
        release_json_asset = next(
            (a for a in release["assets"] if a["name"] == "release.json"), None
        )
        if release_json_asset is not None:
            release_json_response = await get_rate_limited(
                partial(client.get, release_json_asset["browser_download_url"])
            )
            release_json = release_json_response.json()
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
            )


async def get_projects(token: str):
    projects: set[Project] = set()

    async with httpx.AsyncClient(
        base_url="https://api.github.com/",
        follow_redirects=True,
        headers={
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {token}",
        },
        limits=httpx.Limits(max_connections=8, max_keepalive_connections=8),
    ) as client:
        async for repos in find_release_json_repos(
            client,
            ["path:.github/workflows bigwigsmods packager", "path:.github/workflows CF_API_KEY"],
        ):
            for project_coro in asyncio.as_completed(
                [parse_repo_has_release_json_releases(client, r) for r in repos]
            ):
                project = await project_coro
                if project is not None:
                    projects.add(project)

    return projects


def main():
    token = os.environ["RELEASE_JSON_ADDONS_GITHUB_TOKEN"]
    projects = asyncio.run(get_projects(token))

    with open("addons.csv", "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(("name", "description", "url", "last_updated", "flavors"))
        csv_writer.writerows(
            (p.name, p.description or "", p.url, p.last_updated.isoformat(), ",".join(p.flavors))
            for p in sorted(projects, key=lambda p: p.url)
        )


if __name__ == "__main__":
    main()
