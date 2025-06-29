from __future__ import annotations

import asyncio
import datetime as dt
import enum
import io
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from contextlib import (
    AbstractAsyncContextManager,
    AsyncExitStack,
    asynccontextmanager,
)
from typing import Any, Literal, NewType, Protocol
from zipfile import ZipFile

import aiohttp
import structlog
from aiohttp_client_cache.cache_control import ExpirationPatterns
from attrs import field, fields, frozen
from cattrs import BaseValidationError, Converter
from yarl import URL

logger = structlog.get_logger()


REPO_EXCLUDES = (
    "alchem1ster/AddOns-Update-Tool",  # Not an add-on
    "alchem1ster/AddOnsFixer",  # Not an add-on
    "Aviana/",
    "BilboTheGreedy/Azerite",  # Not an add-on
    "blazer404/TargetCharmsRe",  # Fork
    "Centias/BankItems",  # Fork
    "DaMitchell/HelloWorld",  # Dummy add-on
    "dratr/BattlePetCount",  # Fork
    "Expensify/App",  # Not an add-on
    "gorilla-devs/",  # Minecraft stuff
    "HappyRot/AddOns",  # Compilation
    "hippuli/",  # Fork galore
    "JsMacros/",  # Minecraft stuff
    "juraj-hrivnak/Underdog",  # Minecraft stuff
    "kamoo1/Kamoo-s-TSM-App",  # Not an add-on
    "Kirri777/WorldQuestsList",  # Fork
    "layday/wow-addon-template",  # Template
    "livepeer/",  # Minecraft stuff
    "lowlee/MikScrollingBattleText",  # Fork
    "lowlee/MSBTOptions",  # Fork
    "Mctalian/wow-build-tools",  # Not an add-on
    "medi8tor/Addons",  # Add-on pack
    "MikeD89/KarazhanChess",  # Hijacking BigWigs' TOC IDs, probably by accident
    "ogri-la/elvui",  # Mirror
    "ogri-la/tukui",  # Mirror
    "Oppzippy/HuokanGoldLogger",  # Archived
    "pinged-eu/wow-addon-helloworld",  # Dummy add-on
    "rePublic-Studios/rPLauncher",  # Minecraft stuff
    "smashedr/MethodAltManager",  # Fork
    "szjunklol/Accountant",  # Fork
    "unix/curseforge-release",  # Template
    "unrealshape/AddOns",  # Add-on compilation
    "vicitafirea/InterfaceColors-Addon",  # Custom client add-on
    "vicitafirea/TimeOfDayIndicator-AddOn",  # Custom client add-on
    "vicitafirea/TurtleHardcoreMessages-AddOn",  # Custom client add-on
    "vicitafirea/WarcraftUI-UpperBar-AddOn",  # Custom client add-on
    "wagyourtail/JsMacros",  # More Minecraft stuff
    "WOWRainbowUI/RainbowUI-Era",  # Massive add-on bundle (from torkus)
    "WOWRainbowUI/RainbowUI-Retail",  # Massive add-on bundle (from torkus)
    "WowUp/WowUp",  # Not an add-on
    "XiconQoo/RETabBinder",  # Fork for dead version of the game
    "ynazar1/Arh",  # Fork
)


USER_AGENT = "github-wow-addon-catalogue (+https://github.com/layday/github-wow-addon-catalogue)"

_API_URL = URL("https://api.github.com/")

_SEARCH_INTERVAL_HOURS = 2

_CACHE_INDEFINITELY = -1
_EXPIRE_URLS: ExpirationPatterns = {
    f"{_API_URL.host}/search": dt.timedelta(hours=_SEARCH_INTERVAL_HOURS),
    f"{_API_URL.host}/repos/*/releases": dt.timedelta(hours=1),
}


class _Get(Protocol):
    def __call__(self, url: str | URL) -> AbstractAsyncContextManager[aiohttp.ClientResponse]: ...


class ReleaseJsonFlavor(enum.StrEnum):
    mainline = "mainline"
    classic = "classic"
    bcc = "bcc"
    wrath = "wrath"
    cata = "cata"
    mists = "mists"


@frozen
class ReleaseJson:
    releases: list[ReleaseJsonRelease]


@frozen
class ReleaseJsonRelease:
    filename: str
    nolib: bool
    metadata: list[ReleaseJsonReleaseMetadata]


@frozen
class ReleaseJsonReleaseMetadata:
    flavor: ReleaseJsonFlavor
    interface: int


_release_json_converter = Converter()


_TOC_ALIASES = {
    **ReleaseJsonFlavor.__members__,
    "vanilla": ReleaseJsonFlavor.classic,
    "tbc": ReleaseJsonFlavor.bcc,
    "wotlkc": ReleaseJsonFlavor.wrath,
}

_top_level_toc_name_pattern = re.compile(
    rf"""
        ^
        (?P<name>[^/]+)
        [/]
        (?P=name)
        (?:[-_](?P<flavor>{"|".join(map(re.escape, _TOC_ALIASES))}))?
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
    range(4_00_00, 4_04_00): ReleaseJsonFlavor.mainline,
    range(4_04_00, 5_00_00): ReleaseJsonFlavor.cata,
    range(5_00_00, 5_05_00): ReleaseJsonFlavor.mainline,
    range(5_05_00, 6_00_00): ReleaseJsonFlavor.mists,
    range(6_00_00, 12_00_00): ReleaseJsonFlavor.mainline,
}


ProjectFlavors = NewType("ProjectFlavors", frozenset[ReleaseJsonFlavor])


@frozen(frozen=True, kw_only=True)
class Project:
    id: str = field(converter=str)
    name: str
    full_name: str
    url: str
    description: str | None
    last_updated: dt.datetime
    flavors: ProjectFlavors
    curse_id: str | None
    wago_id: str | None
    wowi_id: str | None
    has_release_json: bool
    last_seen: dt.datetime

    @classmethod
    def from_csv_row(cls, values: object):
        return _addons_csv_converter.structure(values, cls)

    def to_csv_row(self):
        return _addons_csv_converter.unstructure(self)


PROJECT_FIELD_NAMES = tuple(f.name for f in fields(Project))

_addons_csv_converter = Converter()
_addons_csv_converter.register_unstructure_hook(dt.datetime, lambda v: v.isoformat())
_addons_csv_converter.register_structure_hook(
    dt.datetime, lambda v, _: dt.datetime.fromisoformat(v)
)
_addons_csv_converter.register_unstructure_hook(ProjectFlavors, lambda v: ",".join(sorted(v)))
_addons_csv_converter.register_structure_hook(
    bool,
    lambda v, _: v == "True",
)
_addons_csv_converter.register_structure_hook(
    ProjectFlavors,
    lambda v, _: [ReleaseJsonFlavor(f) for f in v.split(",") if f],
)


def _parse_toc_file(contents: str):
    return {
        k: v
        for e in contents.splitlines()
        if e.startswith("##")
        for k, v in ((i.strip() for i in e.lstrip("#").partition(":")[::2]),)
        if k
    }


def _extract_interface_numbers(interfaces: str):
    for interface in interfaces.split(","):
        try:
            yield int(interface)
        except ValueError:
            continue


def _interface_numbers_to_flavours(interface_numbers: Iterable[int]):
    for interface_number in interface_numbers:
        for interface_range, flavor in INTERFACE_RANGES_TO_FLAVORS.items():
            if interface_number in interface_range:
                yield flavor
                break


async def _find_addon_repos(
    get: _Get,
    queries_by_endpoint: Sequence[
        tuple[Literal["code", "repositories", "user_repositories"], str]
    ],
):
    for endpoint, query in queries_by_endpoint:
        search_url = (
            (_API_URL / "users" / query / "repos")
            if endpoint == "user_repositories"
            else (_API_URL / "search" / endpoint).with_query(q=query)
        )
        search_url = search_url.extend_query(per_page=100)
        while True:
            async with get(search_url) as response:
                content = await response.json()

            match endpoint:
                case "code":
                    for item in content["items"]:
                        yield item["repository"]
                case "repositories":
                    for item in content["items"]:
                        yield item
                case "user_repositories":
                    for item in content:
                        yield item

            next_url = response.links.get("next")
            if next_url is None:
                break
            search_url = next_url["url"]


async def _extract_project_ids_from_toc_files(get: _Get, url: str, zip_filename: str):
    async with get(url) as file_response:
        file = await file_response.read()

    def is_valid_toc(toc_filename: str):
        match = _top_level_toc_name_pattern.match(toc_filename)
        if match:
            return zip_filename.startswith(match["name"].lstrip("!_"))

    with ZipFile(io.BytesIO(file)) as addon_zip:
        toc_files_contents = [
            addon_zip.read(n).decode("utf-8-sig") for n in addon_zip.namelist() if is_valid_toc(n)
        ]

    if toc_files_contents:
        tocs = (_parse_toc_file(c) for c in toc_files_contents)
        toc_ids = (
            (
                t.get("X-Curse-Project-ID"),
                t.get("X-Wago-ID"),
                t.get("X-WoWI-ID"),
            )
            for t in tocs
        )
        toc_ids_by_source: zip[tuple[str, ...]] = zip(*toc_ids)
        project_ids = (next(filter(None, s), None) for s in toc_ids_by_source)

    else:
        logger.warning(f"unable to find conformant TOC files in {url}")
        project_ids = (None,) * 3

    return dict(zip(("curse_id", "wago_id", "wowi_id"), project_ids))


async def _extract_game_flavors_from_tocs(get: _Get, release_archives: Sequence[dict[str, Any]]):
    for release_archive in release_archives:
        async with get(release_archive["browser_download_url"]) as file_response:
            file = await file_response.read()

        with ZipFile(io.BytesIO(file)) as addon_zip:
            toc_names = [
                (n, _TOC_ALIASES.get(m["flavor"]))
                for n in addon_zip.namelist()
                for m in (_top_level_toc_name_pattern.match(n),)
                if m
            ]
            yield (f for _, f in toc_names if f is not None)

            flavorless_toc_names = [n for n, f in toc_names if f is None]
            if flavorless_toc_names:
                tocs = (
                    _parse_toc_file(addon_zip.read(n).decode("utf-8-sig"))
                    for n in flavorless_toc_names
                )
                interface_numbers = (
                    i
                    for t in tocs
                    for s in (t.get("Interface"),)
                    if s
                    for i in _extract_interface_numbers(s)
                )
                yield _interface_numbers_to_flavours(interface_numbers)


async def _parse_repo(get: _Get, repo: Mapping[str, Any]):
    async with get(
        (_API_URL / "repos" / repo["full_name"] / "releases").with_query(per_page=1)
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
                release_json = _release_json_converter.structure(
                    release_json_contents, ReleaseJson
                )
            except BaseValidationError:
                logger.exception(
                    "release.json has incorrect schema:"
                    f" {maybe_release_json_asset['browser_download_url']}"
                )
                return

            flavors = frozenset(m.flavor for r in release_json.releases for m in r.metadata)
            release_json_release = release_json.releases[0]
            try:
                project_ids = await _extract_project_ids_from_toc_files(
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
                {
                    f
                    async for a in _extract_game_flavors_from_tocs(get, release_archives)
                    for f in a
                }
            )
            release_archive = release_archives[0]
            project_ids = await _extract_project_ids_from_toc_files(
                get, release_archive["browser_download_url"], release_archive["name"]
            )

        return Project(
            id=repo["id"],
            name=repo["name"],
            full_name=repo["full_name"],
            url=repo["html_url"],
            description=repo["description"],
            last_updated=dt.datetime.fromisoformat(f"{release['published_at'].rstrip('Z')}+00:00"),
            flavors=ProjectFlavors(flavors),
            has_release_json=maybe_release_json_asset is not None,
            last_seen=dt.datetime.now(dt.UTC),
            **project_ids,
        )


@asynccontextmanager
async def _make_http_client(token: str):
    from aiohttp_client_cache import CacheBackend
    from aiohttp_client_cache.session import CachedSession

    from ._http_cache import make_cache

    async with AsyncExitStack() as exit_stack:
        cache_backend = CacheBackend(
            autoclose=False,
            cache_name="http-cache.db",
            expire_after=_CACHE_INDEFINITELY,
            urls_expire_after=_EXPIRE_URLS,
            timeout=20,
        )
        cache_backend.responses, cache_backend.redirects = await exit_stack.enter_async_context(
            make_cache()
        )
        client = await exit_stack.enter_async_context(
            CachedSession(
                cache=cache_backend,
                connector=aiohttp.TCPConnector(limit_per_host=8),
                headers={
                    "Accept": "application/vnd.github.v3+json",
                    "Authorization": f"token {token}",
                    "User-Agent": USER_AGENT,
                },
                timeout=aiohttp.ClientTimeout(sock_connect=10, sock_read=10),
            )
        )

        @asynccontextmanager
        async def get(url: str | URL, **kwargs: Any):
            for _ in range(5):
                async with client.get(url, **kwargs) as response:
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
                            sleep_until = dt.datetime.fromtimestamp(  # noqa: DTZ006
                                int(response.headers["X-RateLimit-Reset"])
                            )
                            sleep_for = max(
                                0,
                                (sleep_until - dt.datetime.now()).total_seconds(),  # noqa: DTZ005
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
                response.raise_for_status()  # pyright: ignore[reportPossiblyUnboundVariable]

        yield (client, get)


async def get_projects(
    *,
    token: str,
    search_params: Sequence[tuple[Literal["code", "repositories", "user_repositories"], str]]
    | None = None,
):
    if search_params is None:
        search_params = [
            ("code", "path:.github/workflows bigwigsmods packager"),
            ("code", "path:.github/workflows CF_API_KEY"),
            ("repositories", "topic:wow-addon"),
            ("repositories", "topic:world-of-warcraft-addon"),
            ("repositories", "topic:warcraft-addon"),
            ("repositories", "topics:>2 topic:world-of-warcraft topic:addon"),
        ]

    async with _make_http_client(token) as (_, get):
        deduped_repos = {
            r["full_name"]: r
            async for r in _find_addon_repos(get, search_params)
            if not r["full_name"].startswith(REPO_EXCLUDES)
        }
        projects = {
            r
            for c in asyncio.as_completed(_parse_repo(get, r) for r in deduped_repos.values())
            for r in (await c,)
            if r
        }
        return projects


async def get_projects_to_prune(
    *, token: str, prune_candidates: Iterable[str]
) -> Mapping[str, Project | Literal[False]]:
    async with _make_http_client(token) as (_, get):

        async def get_repo(candidate: str):
            try:
                async with get(_API_URL / "repos" / candidate) as get_response:
                    repo = await get_response.json()
                    return (candidate, (await _parse_repo(get, repo)) or False)
            except aiohttp.ClientResponseError:
                return (candidate, False)

        return {
            f: s
            for c in asyncio.as_completed(get_repo(f) for f in prune_candidates)
            for (f, s) in (await c,)
        }
