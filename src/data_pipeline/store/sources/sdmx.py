"""One SDMX source class for the BIS, the ECB, Eurostat, the OECD and the IMF. No key.

A series id is `<flow>/<key>`, split at the first `/`:

    bis       WS_TC/Q.AR.P.A.M.770.A
    ecb       FM/B.U2.EUR.4F.KR.MRR_FR.LEV
    eurostat  une_rt_m/M.SA.TOTAL.PC_ACT.T.AT
    oecd      OECD.SDD.STES,DSD_STES@DF_CLI/AUT.M.BCICP.IX._Z.AA.IX._Z.H
    imf       IMF.RES,WEO/ARG.GGXCNL_NGDP.A

Every provider is asked for CSV and answers with the columns TIME_PERIOD and OBS_VALUE. The key
of an id must select exactly one series: a key with an open dimension returns several and is
refused. IMF WEO years after LATEST_ACTUAL_ANNUAL_DATA are projections.

Series of one flow that differ in a single position of the key (usually the country) are asked
for in one call, with the values of that position joined by `+`:

    WS_TC/Q.AR.P.A.M.770.A, WS_TC/Q.BR.P.A.M.770.A  ->  WS_TC/Q.AR+BR.P.A.M.770.A

The answer is split back into its series by the column that carries those values. A group that
cannot be read as a group is asked for series by series.
"""

import csv
import dataclasses
import datetime
import io
import re
from collections.abc import Iterator, Mapping, Sequence

import httpx

from data_pipeline.store.errors import (
    CatalogError,
    NetworkError,
    PeriodError,
    QuotaExhaustedError,
    RateLimitedError,
)
from data_pipeline.store.http import Client
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Kind,
    Observation,
    Outcome,
    Request,
    SeriesData,
)
from data_pipeline.store.periods import infer_frequency, read_period
from data_pipeline.store.sources.base import UNSUPPORTED_FREQUENCY, failures, number, reject_params

OK = 200
NOT_FOUND = frozenset({400, 404})
UNIT_COLUMNS = ("UNIT_MEASURE", "UNIT", "unit")
MEASURE_COLUMNS = frozenset({"TIME_PERIOD", "OBS_VALUE"})
NO_OBSERVATIONS = "the query returned no observations"
SEVERAL_SERIES = "the key returns several series: fix every dimension"
MAX_PER_CALL = 50  # series in one grouped call: keeps the address short
OR = "+"
_YEAR = re.compile(r"\d{4}")  # WEO latest actual year: "2024", or fiscal "FY2024/25" -> first year

Row = dict[str, str]


@dataclasses.dataclass(frozen=True)
class Provider:
    url: str  # with {flow} and {key}
    params: Mapping[str, str]
    per_minute: int  # assumed, not taken from the provider's documentation
    headers: Mapping[str, str] = dataclasses.field(default_factory=dict)


PROVIDERS: Mapping[str, Provider] = {
    "bis": Provider(
        "https://stats.bis.org/api/v1/data/{flow}/{key}/all",
        {"format": "csv", "detail": "dataonly"},
        30,
    ),
    "ecb": Provider(
        "https://data-api.ecb.europa.eu/service/data/{flow}/{key}",
        {"format": "csvdata", "detail": "dataonly"},
        30,
    ),
    "eurostat": Provider(
        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{flow}/{key}",
        {"format": "SDMX-CSV"},
        30,
    ),
    # About 60 requests an hour, inferred from its 429 answers on 2026-10-05: 55 calls went
    # through at 20 a minute and every later one was refused.
    "oecd": Provider("https://sdmx.oecd.org/public/rest/data/{flow}/{key}", {"format": "csvfile"}, 1),
    "imf": Provider(
        "https://api.imf.org/external/sdmx/2.1/data/{flow}/{key}",
        {},
        20,
        {"Accept": "application/vnd.sdmx.data+csv;version=1.0.0"},
    ),
}


@dataclasses.dataclass(frozen=True)
class Group:
    """Requests asked for in one call. `position` is the part of the key that varies among them;
    it is None when the group is a single request, asked for with its own key."""

    flow: str
    parts: tuple[str, ...]  # the key of the first member, split at the dots
    position: int | None
    members: tuple[Request, ...]


def split_id(entry: CatalogEntry) -> tuple[str, str]:
    """(flow, key) of a series id such as `WS_TC/Q.AR.P.A.M.770.A`."""
    flow, separator, key = entry.source_id.partition("/")
    if not separator or not flow or not key:
        msg = f"{entry.key}: an SDMX id is '<flow>/<key>', such as WS_TC/Q.AR.P.A.M.770.A"
        raise CatalogError(msg)
    return flow, key


def group_requests(requests: Sequence[Request]) -> list[Group]:
    """Requests of one flow that differ in a single position of the key, put together.

    Among the positions of the key, the one that leaves the fewest distinct keys when ignored is
    taken as the varying one. A key that already holds `+` or an empty part goes alone.
    """
    alone: list[Group] = []
    shapes: dict[tuple[str, int], list[tuple[Request, tuple[str, ...]]]] = {}
    for request in requests:
        flow, key = split_id(request.entry)
        parts = tuple(key.split("."))
        if OR in key or "" in parts:
            alone.append(Group(flow, parts, None, (request,)))
        else:
            shapes.setdefault((flow, len(parts)), []).append((request, parts))
    grouped: list[Group] = []
    for (flow, size), members in shapes.items():
        best: dict[tuple[str, ...], list[int]] = {}
        position = 0
        for candidate in range(size):
            buckets: dict[tuple[str, ...], list[int]] = {}
            for index, (_, parts) in enumerate(members):
                buckets.setdefault(parts[:candidate] + parts[candidate + 1 :], []).append(index)
            if not best or len(buckets) < len(best):
                best, position = buckets, candidate
        for indexes in best.values():
            for start in range(0, len(indexes), MAX_PER_CALL):
                chunk = indexes[start : start + MAX_PER_CALL]
                grouped.append(
                    Group(
                        flow,
                        members[chunk[0]][1],
                        position if len(chunk) > 1 else None,
                        tuple(members[index][0] for index in chunk),
                    )
                )
    return [*grouped, *alone]


def rows_of(text: str) -> list[Row]:
    """The rows of an SDMX-CSV answer that carry a period (the IMF answers an unknown key with
    one empty row)."""
    return [row for row in csv.DictReader(io.StringIO(text)) if (row.get("TIME_PERIOD") or "").strip()]


def splitting_column(rows: Sequence[Row], values: Sequence[str]) -> str | None:
    """The column that tells the series of a grouped answer apart: the one whose values are all
    among the requested ones. None when no column qualifies or two qualify equally."""
    wanted = set(values)
    candidates = []
    for column in rows[0]:
        if column in MEASURE_COLUMNS:
            continue
        seen = {row[column] for row in rows}
        if seen <= wanted:
            candidates.append((len(seen), column))
    candidates.sort(reverse=True)
    if not candidates or (len(candidates) > 1 and candidates[0][0] == candidates[1][0]):
        return None
    return candidates[0][1]


def read_rows(rows: Sequence[Row], entry: CatalogEntry) -> SeriesData | Failure:
    """The one series these rows hold, or the failure they amount to."""
    if not rows:
        return Failure(entry, Outcome.NOT_FOUND, NO_OBSERVATIONS)
    first = rows[0]
    spelling = first["TIME_PERIOD"].strip()
    frequency = infer_frequency(spelling)
    if frequency is None:
        return Failure(entry, Outcome.SOURCE_ERROR, f"{UNSUPPORTED_FREQUENCY} {spelling!r}")
    observations: dict[str, Observation] = {}
    for row in rows:
        period, day = read_period(row["TIME_PERIOD"], frequency)
        if period in observations:
            return Failure(entry, Outcome.SOURCE_ERROR, SEVERAL_SERIES)
        latest_actual = _YEAR.search(row.get("LATEST_ACTUAL_ANNUAL_DATA") or "")
        projection = latest_actual is not None and day.year > int(latest_actual[0])
        observations[period] = Observation(period, day, number(row.get("OBS_VALUE")), projection)
    return SeriesData(
        entry=entry,
        key=entry.key,
        name=(first.get("SERIES_NAME") or "").strip() or entry.source_id,
        frequency=frequency,
        units=next((first[column] for column in UNIT_COLUMNS if first.get(column)), ""),
        observations=tuple(sorted(observations.values(), key=lambda observation: observation.date)),
    )


def read_safely(rows: Sequence[Row], entry: CatalogEntry) -> SeriesData | Failure:
    """`read_rows`, with rows that cannot be parsed turned into the failure of this one series."""
    try:
        return read_rows(rows, entry)
    except (KeyError, IndexError, TypeError, ValueError, PeriodError) as exc:
        return Failure(entry, Outcome.SOURCE_ERROR, f"unexpected answer ({type(exc).__name__}: {exc})")


def read_csv(text: str, entry: CatalogEntry) -> SeriesData | Failure:
    """The one series of an SDMX-CSV answer, or the failure the answer amounts to."""
    return read_safely(rows_of(text), entry)


def batch(results: Sequence[SeriesData | Failure]) -> FetchBatch:
    return FetchBatch(
        tuple(item for item in results if isinstance(item, SeriesData)),
        tuple(item for item in results if isinstance(item, Failure)),
    )


class Sdmx:
    kind = Kind.SERIES
    daily_budget: int | None = None

    def __init__(self, name: str, client: Client) -> None:
        self.name = name
        self._provider = PROVIDERS[name]
        self.requests_per_minute = self._provider.per_minute
        self._client = client

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)
        split_id(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        for group in group_requests(requests):
            try:
                yield self._group(group)
            except RateLimitedError as exc:
                # Asking again would only prolong the block: stop here, the next run resumes.
                raise QuotaExhaustedError(str(exc)) from exc
            except NetworkError as exc:
                yield FetchBatch(failures=failures(group.members, Outcome.NETWORK_ERROR, str(exc)))
            except (KeyError, IndexError, TypeError, ValueError, PeriodError) as exc:
                reason = f"unexpected answer ({type(exc).__name__}: {exc})"
                yield FetchBatch(failures=failures(group.members, Outcome.SOURCE_ERROR, reason))

    def _group(self, group: Group) -> FetchBatch:
        if group.position is None:
            return batch([self._download(group.members[0])])
        position = group.position
        values = [split_id(request.entry)[1].split(".")[position] for request in group.members]
        key = ".".join((*group.parts[:position], OR.join(values), *group.parts[position + 1 :]))
        every_since = [request.since for request in group.members if request.since is not None]
        since = min(every_since) if len(every_since) == len(group.members) else None
        response = self._get(group.flow, key, since)
        if response.status_code != OK:
            return self._one_by_one(group)  # one bad value can make the provider refuse them all
        rows = rows_of(response.text)
        if not rows:
            return FetchBatch(failures=failures(group.members, Outcome.NOT_FOUND, NO_OBSERVATIONS))
        column = splitting_column(rows, values)
        if column is None:
            return self._one_by_one(group)
        own: dict[str, list[Row]] = {}
        for row in rows:
            own.setdefault(row[column], []).append(row)
        return batch(
            [
                read_safely(own.get(value, []), request.entry)
                for request, value in zip(group.members, values, strict=True)
            ]
        )

    def _one_by_one(self, group: Group) -> FetchBatch:
        """The safety net: each request of the group asked for with its own key."""
        results: list[SeriesData | Failure] = []
        for request in group.members:
            try:
                results.append(self._download(request))
            except RateLimitedError:
                raise
            except NetworkError as exc:
                results.append(Failure(request.entry, Outcome.NETWORK_ERROR, str(exc)))
        return batch(results)

    def _download(self, request: Request) -> SeriesData | Failure:
        entry = request.entry
        flow, key = split_id(entry)
        response = self._get(flow, key, request.since)
        if response.status_code != OK:
            outcome = Outcome.NOT_FOUND if response.status_code in NOT_FOUND else Outcome.SOURCE_ERROR
            return Failure(entry, outcome, f"HTTP {response.status_code}: {self._client.excerpt(response.text, 200)}")
        return read_csv(response.text, entry)

    def _get(self, flow: str, key: str, since: datetime.date | None) -> httpx.Response:
        params = dict(self._provider.params)
        if since is not None:
            params["startPeriod"] = str(since.year)
        return self._client.get(
            self.name,
            self._provider.url.format(flow=flow, key=key),
            params=params,
            headers=self._provider.headers,
            per_minute=self.requests_per_minute,
        )
