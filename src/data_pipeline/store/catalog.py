"""The catalog: what the user wants downloaded. A YAML list of entries, or entries built in code.

Several ids that share their fields:

    - source: fred
      ids: [UNRATE, DGS10]
      alias:
        UNRATE: usa.empleo.desempleo
      start: 1990-01-01

One series with fields of its own:

    - source: dbnomics
      id: Eurostat/prc_hicp_midx/M.I15.CP00.EA
      alias: e_ea_hicp
      name: Euro area HICP (index, 2015=100)
      frequency: M

Fields every source shares: source, ids or id, alias, name, frequency, start, stale_after_days,
attrs. Any other field belongs to the source and is checked by its `validate`.

A catalog can also be one of those shipped with the library, named without a path: "macro".
"""

import datetime
import importlib.resources
import pathlib
from collections.abc import Mapping, Sequence
from typing import Any

import yaml

from data_pipeline.store.errors import CatalogError
from data_pipeline.store.model import CatalogEntry, Frequency
from data_pipeline.store.sources.base import Source

SHARED_FIELDS = frozenset(
    {"source", "ids", "id", "alias", "name", "frequency", "start", "stale_after_days", "attrs"}
)
BUNDLED_PACKAGE = "data_pipeline.store.catalogs"
BUNDLED_SUFFIX = ".yaml"


def _fail(where: str, problem: str) -> CatalogError:
    return CatalogError(f"{where}: {problem}")


def _frequency(value: object, where: str) -> Frequency | None:
    if value is None or isinstance(value, Frequency):
        return value
    try:
        return Frequency(str(value))
    except ValueError:
        allowed = ", ".join(item.value for item in Frequency)
        raise _fail(where, f"'frequency' must be one of {allowed}") from None


def build_entries(
    source: str,
    ids: Sequence[object],
    *,
    where: str = "catalog",
    alias: Mapping[str, str] | None = None,
    name: str | None = None,
    frequency: object = None,
    start: datetime.date | None = None,
    stale_after_days: int | None = None,
    attrs: Mapping[str, str] | None = None,
    params: Mapping[str, object] | None = None,
) -> tuple[CatalogEntry, ...]:
    """One CatalogEntry per id. Ids are turned into text (YAML reads 737121 as a number)."""
    if not isinstance(source, str) or not source:
        raise _fail(where, "'source' must be a non-empty text")
    if isinstance(ids, str) or not isinstance(ids, Sequence) or not ids:
        raise _fail(where, "'ids' must be a non-empty list")
    names = [str(item) for item in ids]
    aliases = dict(alias or {})
    unknown = sorted(set(map(str, aliases)) - set(names))
    if unknown:
        raise _fail(where, f"'alias' names ids that are not in 'ids': {', '.join(unknown)}")
    if name is not None and len(names) > 1:
        raise _fail(where, "'name' fits one series: use 'id' instead of 'ids'")
    if start is not None and not isinstance(start, datetime.date):
        raise _fail(where, "'start' must be a date such as 1990-01-01")
    if stale_after_days is not None and (isinstance(stale_after_days, bool) or not isinstance(stale_after_days, int)):
        raise _fail(where, "'stale_after_days' must be a whole number")
    declared = _frequency(frequency, where)
    return tuple(
        CatalogEntry(
            source=source,
            source_id=item,
            alias=str(aliases[item]) if item in aliases else None,
            start=start,
            stale_after_days=stale_after_days,
            attrs={str(k): str(v) for k, v in (attrs or {}).items()},
            params=dict(params or {}),
            name=None if name is None else str(name),
            frequency=declared,
        )
        for item in names
    )


def _ids_and_alias(fields: Mapping[str, Any], where: str) -> tuple[Sequence[object], dict[str, str]]:
    """Read either spelling: `ids` with an alias mapping, or `id` with an alias text."""
    if "ids" in fields and "id" in fields:
        raise _fail(where, "use 'ids' or 'id', not both")
    alias = fields.get("alias")
    if "id" in fields:
        if isinstance(fields["id"], list | dict) or fields["id"] in (None, ""):
            raise _fail(where, "'id' must be one series id")
        if isinstance(alias, list | dict):
            raise _fail(where, "with 'id', 'alias' must be a text")
        one = str(fields["id"])
        return [one], ({} if alias is None else {one: str(alias)})
    if "ids" not in fields:
        raise _fail(where, "missing 'ids' (or 'id')")
    if alias is not None and not isinstance(alias, dict):
        raise _fail(where, "'alias' must be a mapping")
    return fields["ids"], {str(k): str(v) for k, v in (alias or {}).items()}


def parse_catalog(raw: object, where: str = "catalog") -> tuple[CatalogEntry, ...]:
    """Turn the parsed YAML (a list of mappings) into entries."""
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise _fail(where, "the catalog must be a list of entries")
    entries: list[CatalogEntry] = []
    for position, item in enumerate(raw, start=1):
        here = f"{where}, entry {position}"
        if not isinstance(item, dict):
            raise _fail(here, "an entry must be a mapping with 'source' and 'ids'")
        fields: dict[str, Any] = {str(k): v for k, v in item.items()}
        if "source" not in fields:
            raise _fail(here, "missing 'source'")
        ids, alias = _ids_and_alias(fields, here)
        if fields.get("attrs") is not None and not isinstance(fields["attrs"], dict):
            raise _fail(here, "'attrs' must be a mapping")
        entries.extend(
            build_entries(
                fields["source"],
                ids,
                where=here,
                alias=alias,
                name=fields.get("name"),
                frequency=fields.get("frequency"),
                start=fields.get("start"),
                stale_after_days=fields.get("stale_after_days"),
                attrs=fields.get("attrs"),
                params={k: v for k, v in fields.items() if k not in SHARED_FIELDS},
            )
        )
    return tuple(entries)


def bundled_catalogs() -> tuple[str, ...]:
    """Names of the catalogs shipped with the library."""
    folder = importlib.resources.files(BUNDLED_PACKAGE)
    names = [item.name for item in folder.iterdir() if item.name.endswith(BUNDLED_SUFFIX)]
    return tuple(sorted(name.removesuffix(BUNDLED_SUFFIX) for name in names))


def _read(reference: pathlib.Path) -> tuple[str, str]:
    """(text, where) of a catalog: an existing file wins, then a bundled catalog of that name."""
    if reference.exists():
        return reference.read_text(encoding="utf-8"), str(reference)
    name = str(reference)
    if name in bundled_catalogs():
        resource = importlib.resources.files(BUNDLED_PACKAGE).joinpath(name + BUNDLED_SUFFIX)
        return resource.read_text(encoding="utf-8"), f"bundled catalog {name!r}"
    known = ", ".join(bundled_catalogs()) or "none"
    raise _fail(str(reference), f"the catalog file does not exist (bundled catalogs: {known})")


def load_catalog(reference: pathlib.Path) -> tuple[CatalogEntry, ...]:
    """Read a YAML catalog: a file, or the name of a catalog shipped with the library."""
    text, where = _read(reference)
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise _fail(where, f"not valid YAML ({exc})") from exc
    return parse_catalog(raw, where=where)


def check_catalog(entries: Sequence[CatalogEntry], sources: Mapping[str, Source]) -> None:
    """Checks that need the whole catalog: duplicates, aliases, and each source's own fields."""
    keys: set[str] = set()
    aliases: set[str] = set()
    for entry in entries:
        if entry.source not in sources:
            msg = f"{entry.key}: unknown source {entry.source!r}"
            raise CatalogError(msg)
        if entry.key in keys:
            msg = f"{entry.key}: declared more than once"
            raise CatalogError(msg)
        keys.add(entry.key)
        if entry.alias is not None:
            if ":" in entry.alias:
                msg = f"{entry.key}: alias {entry.alias!r} may not contain ':'"
                raise CatalogError(msg)
            if entry.alias in aliases:
                msg = f"{entry.key}: alias {entry.alias!r} is used more than once"
                raise CatalogError(msg)
            aliases.add(entry.alias)
        sources[entry.source].validate(entry)
