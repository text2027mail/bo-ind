#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BFILMY Movie JSON Builder — GitHub Actions edition
====================================================

Two run modes:

  FULL        First run (or after --fresh / interrupted run):
              fetch every day from START_DATE through today plus
              the 45-day advance horizon. Filter by state.json so
              already-fetched days are skipped.

  INCREMENTAL Once every historical day is present in state.json:
              fetch only the last INCREMENTAL_PAST_DAYS daily
              files plus today and the next ADVANCE_FUTURE_DAYS
              in both daily and advance mode.

JSON rebuild is change-driven in both modes: only movies that had
data written during the current run get their JSON file rewritten.

Output metric format (positional arrays, no repeated keys):

  Standard metric — 8 values:
      [g, o, sh, ff, hf, t, se, z]
      g  = gross          o  = occupancy %       sh = shows
      ff = fast-filling   hf = housefull         t  = tickets
      se = total seats    z  = zero-gross shows

  Timewise metric — 7 values:
      [sh, ff, hf, t, g, o, atp]

Run:
    python3 bfilmy_builder.py
    python3 bfilmy_builder.py --fresh
    python3 bfilmy_builder.py --rebuild
    python3 bfilmy_builder.py --refetch-month 2026-05
    python3 bfilmy_builder.py --force-full
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import contextlib
import datetime as dt
import gc
import json
import logging
import os
import re
import resource
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import unicodedata
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

import aiohttp

try:
    import ijson  # type: ignore
except ImportError:
    ijson = None

try:
    import uvloop  # type: ignore
    uvloop.install()
    _HAS_UVLOOP = True
except ImportError:
    _HAS_UVLOOP = False


# ============================================================
# CONFIG
# ============================================================

# GitHub Actions runners: 7 GB RAM, 2 cores, ephemeral disk.
MAX_CONCURRENCY = 80
PARSE_THREADS = 6
DB_QUEUE_DEPTH = 24

FLUSH_EVERY_N_FILES = 40
FLUSH_MAX_ROWS = 75_000

# Live checkpoint publishing: build/push dirty movie JSONs every N completed fetch jobs.
LIVE_PUSH_EVERY = 20
LIVE_BUILD_WORKERS = 2

REQUEST_TIMEOUT = 90
CONNECT_TIMEOUT = 20
RETRIES = 3
DOWNLOAD_CHUNK_SIZE = 512 * 1024

START_DATE = dt.date(2023, 1, 1)
ADVANCE_FUTURE_DAYS = 45

# In incremental mode, refetch this many past days. Data for recent
# days can still be revised by the source.
INCREMENTAL_PAST_DAYS = 5

FILE_WRITE_BUFFER = 256 * 1024

OUTPUT_DIR = Path("data")
TMP_DIR = OUTPUT_DIR / "_tmp"
STATE_PATH = TMP_DIR / "state.json"
DB_PATH = TMP_DIR / "bfilmy.sqlite3"
LOG_PATH = TMP_DIR / "bfilmy_builder.log"

TMP_DIR.mkdir(parents=True, exist_ok=True)

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
STATE_VERSION = 1


# ============================================================
# LOGGING
# ============================================================

log = logging.getLogger("bfilmy")


def setup_logging(verbose: bool = False) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    log.setLevel(logging.DEBUG)
    for h in list(log.handlers):
        log.removeHandler(h)

    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    log.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(ch)


def now_iso() -> str:
    return dt.datetime.now(IST).isoformat(timespec="seconds")


def rss_mb() -> float:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except Exception:
        pass
    try:
        import resource as _r
        return _r.getrusage(_r.RUSAGE_SELF).ru_maxrss / (1024.0 * 1024.0)
    except Exception:
        return 0.0


def detect_ijson_backend() -> str:
    if ijson is None:
        return "not installed"
    for name, label in (
        ("yajl2_c", "yajl2_c (fastest)"),
        ("yajl2", "yajl2 (fast)"),
        ("yajl", "yajl (fast)"),
    ):
        try:
            __import__(f"ijson.backends.{name}")
            return label
        except ImportError:
            continue
    return "python (SLOW)"


# ============================================================
# DATE / TIME HELPERS
# ============================================================

def today_ist() -> dt.date:
    return dt.datetime.now(IST).date()


def get_time_slot(value: Any) -> str | None:
    """
    Match FinalDetailedJson.js getTimeSlot() + canonSlot() exactly:

      04:00-11:59 -> M  (Early Morning + Morning)
      12:00-14:59 -> A  (Noon)
      15:00-18:59 -> E  (Evening)
      19:00-03:59 -> N  (Night + Late Night)

    Invalid/missing showtimes are ignored.
    """
    if value is None:
        return None
    text = str(value).strip().upper()
    minute: int | None = None

    for fmt in ("%I:%M %p", "%H:%M", "%I %p"):
        try:
            parsed = dt.datetime.strptime(text, fmt)
            minute = parsed.hour * 60 + parsed.minute
            break
        except ValueError:
            pass

    if minute is None:
        return None

    if 4 * 60 <= minute < 12 * 60:
        return "M"
    if 12 * 60 <= minute < 15 * 60:
        return "A"
    if 15 * 60 <= minute < 19 * 60:
        return "E"
    return "N"


# ============================================================
# STATE
# ============================================================

class State:
    def __init__(self, path: Path):
        self.path = path
        self.created = now_iso()
        self.last_run = self.created
        self.days: dict[str, dict[str, bool]] = {}
        self.dirty: set[str] = set()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("version") != STATE_VERSION:
                log.warning("State version mismatch; starting fresh")
                return
            self.created = data.get("created", self.created)
            self.last_run = data.get("last_run", self.last_run)
            self.days = data.get("days", {}) or {}
            self.dirty = set(data.get("dirty", []) or [])
        except Exception as e:
            log.warning("Could not load state (%s); starting fresh", e)
            self.days = {}
            self.dirty = set()

    def save(self) -> None:
        self.last_run = now_iso()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump({
                "version": STATE_VERSION,
                "created": self.created,
                "last_run": self.last_run,
                "days": self.days,
                "dirty": sorted(self.dirty),
            }, f, indent=1, sort_keys=True)
        os.replace(tmp, self.path)

    @staticmethod
    def _letter(mode: str) -> str:
        return "d" if mode == "daily" else "a"

    def has(self, date: dt.date, mode: str) -> bool:
        rec = self.days.get(date.isoformat())
        return bool(rec and rec.get(self._letter(mode), False))

    def mark_many(self, items: list[tuple[dt.date, str]]) -> None:
        for date, mode in items:
            rec = self.days.setdefault(date.isoformat(), {})
            rec[self._letter(mode)] = True

    def clear_month(self, year: int, month: int) -> int:
        prefix = f"{year:04d}-{month:02d}-"
        removed = 0
        for k in list(self.days.keys()):
            if k.startswith(prefix):
                del self.days[k]
                removed += 1
        return removed


    def mark_dirty(self, movies: set[str]) -> None:
        if movies:
            self.dirty.update(movies)

    def clear_dirty(self, movies: set[str]) -> None:
        if movies:
            self.dirty.difference_update(movies)


# ============================================================
# HISTORICAL COMPLETENESS CHECK
# ============================================================

def is_historical_complete(state: State, today: dt.date) -> bool:
    """
    True when every fetchable daily date from START_DATE up to yesterday
    is present in state.json. Used to decide full vs. incremental mode.
    """
    end = today - dt.timedelta(days=1)
    d = START_DATE
    missing = 0
    while d <= end:
        if source_url(d, "daily") and not state.has(d, "daily"):
            missing += 1
            if missing > 25:   # short-circuit — no need to count the whole gap
                return False
        d += dt.timedelta(days=1)
    if missing:
        log.debug("historical check: %d daily dates missing", missing)
        return False
    return True


# ============================================================
# SOURCE ROUTER
# ============================================================

def source_url(d: dt.date, mode: str) -> str | None:
    """
    Canonical source map based on the two supplied JS readers.

    DAILY:
      <= 2025-12-31 : finalsummary archive
      2026-01       : finalsummary archive
      2026-02 onward:
          recent (yesterday/today) -> live finaldetailed
          older             -> yearly archive finaldetailed

    ADVANCE:
      <= 2025-12-31 : not available
      2026-01       : finaldetailed archive
      2026-02 onward:
          recent/future -> live finaldetailed
          older         -> yearly archive finaldetailed
    """
    today = today_ist()

    # No advance archive exists for <= 2025 in the supplied routing model.
    if mode == "advance" and d.year <= 2025:
        return None

    # 2025 and earlier: daily is summary format.
    if mode == "daily" and d.year <= 2025:
        return (
            "https://bfilmyapi2025.pages.dev/"
            f"daily/data/{d.year}/{d:%m-%d}_finalsummary.json"
        )

    # January 2026 is the special archive:
    # daily = finalsummary, advance = finaldetailed.
    if d == dt.date(2026, 1, 1) or (d.year == 2026 and d.month == 1):
        if mode == "daily":
            return (
                "https://bfilmyapi2026.pages.dev/"
                f"daily/data/2026/{d:%m-%d}_finalsummary.json"
            )
        return (
            "https://bfilmyapi2026.pages.dev/"
            f"advance/data/2026/{d:%m-%d}_finaldetailed.json"
        )

    # The supplied detailed reader keeps the live endpoint for dates
    # within the recent 30-day window and switches to the yearly archive
    # only once the date is more than one month old.
    if (today - d).days <= 30:
        return (
            "https://bfilmyapi.pages.dev/"
            f"{mode}/data/{d:%Y%m%d}/finaldetailed.json"
        )

    # Historical 2026 archive uses year/MM-DD_finaldetailed.
    return (
        "https://bfilmyapi2026.pages.dev/"
        f"{mode}/data/2026/{d:%m-%d}_finaldetailed.json"
    )


# ============================================================
# NUMBER HELPERS
# ============================================================

def to_float(v: Any) -> float:
    try:
        if v in (None, ""):
            return 0.0
        if isinstance(v, str):
            v = v.replace(",", "").strip()
        return float(v)
    except Exception:
        return 0.0


def to_int(v: Any) -> int:
    try:
        if v in (None, ""):
            return 0
        if isinstance(v, str):
            v = v.replace(",", "").strip()
        return int(float(v))
    except Exception:
        return 0


def occupancy(t, s) -> float:
    return 0.0 if s <= 0 else round(t / s * 100, 2)


def atp(g, t) -> float:
    return 0.0 if t <= 0 else round(g / t, 2)


# ============================================================
# SLUG / VARIANT
# ============================================================

def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower().strip()
    text = re.sub(r"\[[^\]]*\]", "", text)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


_BRACKET_RE = re.compile(r"\[([^\]]+)\]\s*$")


def parse_movie_variant(raw: str) -> tuple[str, str, str]:
    raw = str(raw).strip()
    m = _BRACKET_RE.search(raw)
    if not m:
        return raw, "", ""
    movie = raw[: m.start()].strip()
    parts = [x.strip() for x in m.group(1).strip().split("|", 1)]
    return (movie, parts[0], parts[1]) if len(parts) == 2 else (movie, parts[0], "")


# ============================================================
# FF / HF
# ============================================================
# IMPORTANT: FinalDetialedJson.js does NOT use the raw "s" field or
# arbitrary status strings for FF/HF. It computes them only from:
#
#   occ = sold / totalSeats * 100
#   HF if occ >= 98
#   FF if 50 <= occ < 98
#
# Keep this identical for detailed input.
# ============================================================

def get_ff_hf(sold: int, seats: int) -> tuple[int, int]:
    if seats <= 0:
        return 0, 0
    occ = sold / seats * 100.0
    if occ >= 98.0:
        return 0, 1
    if occ >= 50.0:
        return 1, 0
    return 0, 0

# ============================================================
# COMPACT METRIC
# ============================================================
# Standard metric order — DO NOT REORDER without bumping the schema.
#     [g, o, sh, ff, hf, t, se, z]
# Timewise metric order.
#     [sh, ff, hf, t, g, o, atp]
# ============================================================


def compact_metric(v) -> list:
    g, t, sh, ff, hf, se, z = v
    return [round(g, 2), int(t), int(sh), int(ff), int(hf), int(se), int(z)]


def write_metric(f, v) -> None:
    f.write(json.dumps(compact_metric(v), **_J))


def compact_timewise(g, t, sh, ff, hf, se) -> list:
    return [round(g, 2), int(t), int(sh), int(ff), int(hf), int(se)]


# ============================================================
# DB
# ============================================================

def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(
        path,
        timeout=180,
        isolation_level=None,
        check_same_thread=False,
    )
    conn.execute("PRAGMA journal_mode=WAL").fetchone()
    conn.execute("PRAGMA synchronous=NORMAL").fetchone()
    conn.execute("PRAGMA temp_store=FILE").fetchone()
    conn.execute("PRAGMA cache_size=-65536").fetchone()
    conn.execute("PRAGMA wal_autocheckpoint=1000").fetchone()
    conn.execute("PRAGMA mmap_size=536870912").fetchone()
    conn.execute("PRAGMA busy_timeout=180000").fetchone()

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS variants (
            movie TEXT NOT NULL,
            format TEXT NOT NULL,
            language TEXT NOT NULL,
            PRIMARY KEY (movie, format, language)
        );

        CREATE TABLE IF NOT EXISTS metrics (
            mode TEXT NOT NULL,
            dim TEXT NOT NULL,
            date TEXT NOT NULL,
            movie TEXT NOT NULL,
            format TEXT NOT NULL,
            language TEXT NOT NULL,
            entity TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL DEFAULT '',
            gross REAL NOT NULL DEFAULT 0,
            tickets INTEGER NOT NULL DEFAULT 0,
            shows INTEGER NOT NULL DEFAULT 0,
            ff INTEGER NOT NULL DEFAULT 0,
            hf INTEGER NOT NULL DEFAULT 0,
            seats INTEGER NOT NULL DEFAULT 0,
            zero_gross INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (mode, dim, date, movie, format, language, entity)
        );

        CREATE INDEX IF NOT EXISTS idx_metrics_lookup
            ON metrics (movie, format, language, mode, dim, entity, date);
    """)
    return conn


UPSERT_SQL = """
INSERT INTO metrics (
    mode, dim, date, movie, format, language, entity, state,
    gross, tickets, shows, ff, hf, seats, zero_gross
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (mode, dim, date, movie, format, language, entity)
DO UPDATE SET
    gross=excluded.gross, tickets=excluded.tickets, shows=excluded.shows,
    ff=excluded.ff, hf=excluded.hf, seats=excluded.seats,
    zero_gross=excluded.zero_gross,
    state = CASE WHEN excluded.state <> '' THEN excluded.state ELSE state END
"""


def _write_batch_sync(conn, lock, items):
    if not items:
        return 0
    total_rows = 0
    with lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            seen: set = set()
            uniq_variants: list[tuple] = []
            for _, _, _, variants in items:
                for v in variants:
                    if v not in seen:
                        seen.add(v)
                        uniq_variants.append(v)

            if uniq_variants:
                conn.executemany(
                    "INSERT OR IGNORE INTO variants (movie, format, language) "
                    "VALUES (?, ?, ?)",
                    uniq_variants,
                )

            delete_pairs = [(m, d.isoformat()) for d, m, _, _ in items]
            conn.executemany(
                "DELETE FROM metrics WHERE mode = ? AND date = ?",
                delete_pairs,
            )

            all_rows: list[tuple] = []
            for _, _, rows, _ in items:
                all_rows.extend(rows)
            total_rows = len(all_rows)

            if all_rows:
                conn.executemany(UPSERT_SQL, all_rows)

            conn.execute("COMMIT")
        except BaseException:
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute("ROLLBACK")
            raise
    return total_rows


# ============================================================
# METRIC BUFFER
# ============================================================

def metric_zero() -> list:
    return [0.0, 0, 0, 0, 0, 0, 0]


def add_metric(t, g, ti, sh, ff, hf, se, z) -> None:
    t[0] += g; t[1] += ti; t[2] += sh; t[3] += ff
    t[4] += hf; t[5] += se; t[6] += z


# ============================================================
# SOURCE PARSERS
# ============================================================

def _iter_detailed(path: str) -> Iterator[dict]:
    """Streaming iterator for {data:[...]} finaldetailed.json."""
    if ijson is not None:
        with open(path, "rb") as f:
            yield from ijson.items(f, "data.item")
        return

    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    for row in payload.get("data", []) or []:
        yield row


def _iter_summary(path: str) -> Iterator[tuple[str, dict]]:
    """Streaming iterator for {movies:{...}} finalsummary.json."""
    if ijson is not None:
        with open(path, "rb") as f:
            yield from ijson.kvitems(f, "movies")
        return

    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    for key, value in (payload.get("movies", {}) or {}).items():
        yield key, value


def _city_entity(city: str, state: str) -> str:
    """
    Use city||state internally so duplicate city names from different
    states never collapse in SQLite. The public JSON writer decodes it.
    """
    return f"{city}||{state}" if state else city


def _split_city_entity(entity: str, state: str = "") -> tuple[str, str]:
    if "||" in entity:
        city, embedded_state = entity.split("||", 1)
        return city, embedded_state or state
    return entity, state


def parse_detailed_sync(path: str, date: dt.date) -> tuple[list, list]:
    """
    Parse finaldetailed.json exactly at show/session level.

    Every raw show contributes:
      - movie/format/language daily metric
      - city+state metric
      - chain metric
      - M/A/E/N timewise metric

    No venue/audi/session_id/available/etc. are retained because the
    target compact movie JSON does not consume those fields.
    """
    date_str = date.isoformat()
    buckets: dict[tuple[str, str, str], dict[str, Any]] = {}

    for row in _iter_detailed(path):
        if not isinstance(row, dict):
            continue

        raw_movie = row.get("movie")
        if not raw_movie:
            continue

        movie, fmt, lang = parse_movie_variant(str(raw_movie))
        # Match FinalDetailed client: invalid movie variant strings are ignored.
        if not movie or not fmt or not lang:
            continue

        key = (movie, fmt, lang)
        bucket = buckets.get(key)
        if bucket is None:
            bucket = {
                "daily": metric_zero(),
                "cities": {},
                "chains": {},
                "times": {slot: metric_zero() for slot in "MAEN"},
            }
            buckets[key] = bucket

        gross = to_float(row.get("gross"))
        tickets = to_int(row.get("sold"))
        seats = to_int(row.get("totalSeats"))
        ff, hf = get_ff_hf(tickets, seats)

        # Keep z because it is already part of the declared compact schema.
        zero_gross = int(gross == 0)
        add_metric(bucket["daily"], gross, tickets, 1, ff, hf, seats, zero_gross)

        city = str(row.get("city") or "").strip()
        state = str(row.get("state") or "").strip()
        if city:
            entity = _city_entity(city, state)
            city_rec = bucket["cities"].get(entity)
            if city_rec is None:
                city_rec = {"state": state, "metric": metric_zero()}
                bucket["cities"][entity] = city_rec
            add_metric(
                city_rec["metric"],
                gross, tickets, 1, ff, hf, seats, zero_gross
            )

        chain = str(row.get("chain") or "").strip()
        if chain:
            chain_rec = bucket["chains"].get(chain)
            if chain_rec is None:
                chain_rec = metric_zero()
                bucket["chains"][chain] = chain_rec
            add_metric(
                chain_rec,
                gross, tickets, 1, ff, hf, seats, zero_gross
            )

        slot = get_time_slot(row.get("time"))
        if slot:
            add_metric(
                bucket["times"][slot],
                gross, tickets, 1, ff, hf, seats, zero_gross
            )

    rows: list[tuple] = []
    variants: list[tuple[str, str, str]] = []

    for (movie, fmt, lang), bucket in buckets.items():
        variants.append((movie, fmt, lang))

        rows.append((
            "__MODE__", "d", date_str, movie, fmt, lang, "", "",
            *bucket["daily"]
        ))

        for city_entity, city_rec in bucket["cities"].items():
            rows.append((
                "__MODE__", "c", date_str, movie, fmt, lang,
                city_entity, city_rec["state"], *city_rec["metric"]
            ))

        for chain, values in bucket["chains"].items():
            rows.append((
                "__MODE__", "ch", date_str, movie, fmt, lang,
                chain, "", *values
            ))

        for slot in "MAEN":
            values = bucket["times"][slot]
            if values[2]:  # shows
                rows.append((
                    "__MODE__", "tm", date_str, movie, fmt, lang,
                    slot, "", *values
                ))

    return rows, variants


def parse_summary_sync(path: str, date: dt.date, mode: str = "daily") -> tuple[list, list]:
    """
    Parse finalsummary.json exactly as the supplied FinalSummary reader expects:
      root.movies[rawMovie] -> movie-level aggregate
      details[]              -> city/state aggregate
      Chain_details[]        -> chain aggregate

    The source already contains FF/HF/shows/seats totals, so those are trusted
    instead of being reconstructed from city rows.
    """
    date_str = date.isoformat()
    rows: list[tuple] = []
    variants: list[tuple[str, str, str]] = []

    for raw_movie, data in _iter_summary(path):
        if not isinstance(data, dict):
            continue

        movie, fmt, lang = parse_movie_variant(str(raw_movie))
        if not movie:
            continue

        variants.append((movie, fmt, lang))

        rows.append((
            mode, "d", date_str, movie, fmt, lang, "", "",
            to_float(data.get("gross")),
            to_int(data.get("sold")),
            to_int(data.get("shows")),
            to_int(data.get("fastfilling")),
            to_int(data.get("housefull")),
            to_int(data.get("totalSeats")),
            0,
        ))

        for item in data.get("details") or []:
            if not isinstance(item, dict):
                continue
            city = str(item.get("city") or "").strip()
            if not city:
                continue
            state = str(item.get("state") or "").strip()
            rows.append((
                mode, "c", date_str, movie, fmt, lang,
                _city_entity(city, state), state,
                to_float(item.get("gross")),
                to_int(item.get("sold")),
                to_int(item.get("shows")),
                to_int(item.get("fastfilling")),
                to_int(item.get("housefull")),
                to_int(item.get("totalSeats")),
                0,
            ))

        for item in data.get("Chain_details") or []:
            if not isinstance(item, dict):
                continue
            chain = str(item.get("chain") or "").strip()
            if not chain:
                continue
            rows.append((
                mode, "ch", date_str, movie, fmt, lang, chain, "",
                to_float(item.get("gross")),
                to_int(item.get("sold")),
                to_int(item.get("shows")),
                to_int(item.get("fastfilling")),
                to_int(item.get("housefull")),
                to_int(item.get("totalSeats")),
                0,
            ))

    return rows, variants


def patch_mode(rows: list[tuple], mode: str) -> None:
    """Replace the temporary __MODE__ marker with daily/advance."""
    target = "daily" if mode == "daily" else "advance"
    for i, row in enumerate(rows):
        if row and row[0] == "__MODE__":
            rows[i] = (target,) + row[1:]


def parse_sync(path: str, date: dt.date, mode: str) -> tuple[list, list]:
    """
    Correct parser dispatch:
      * 2025 and earlier daily -> finalsummary
      * January 2026 daily      -> finalsummary
      * everything else         -> finaldetailed
    """
    if mode == "daily" and date <= dt.date(2026, 1, 31):
        return parse_summary_sync(path, date, mode=mode)

    rows, variants = parse_detailed_sync(path, date)
    patch_mode(rows, mode)
    return rows, variants


# ============================================================
# ASYNC DOWNLOAD
# ============================================================

async def download_one(session, date, mode, url):
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{date.isoformat()}_{mode}_{uuid.uuid4().hex}.json"
    path = TMP_DIR / filename

    for attempt in range(1, RETRIES + 1):
        try:
            async with session.get(url) as r:
                if r.status == 404:
                    return date, mode, None
                if r.status >= 400:
                    raise aiohttp.ClientResponseError(
                        r.request_info, r.history,
                        status=r.status, message=r.reason,
                    )
                with path.open("wb", buffering=FILE_WRITE_BUFFER) as f:
                    async for chunk in r.content.iter_chunked(DOWNLOAD_CHUNK_SIZE):
                        f.write(chunk)
            return date, mode, str(path)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            with contextlib.suppress(OSError):
                path.unlink(missing_ok=True)
            if attempt < RETRIES:
                await asyncio.sleep(0.5 * attempt)
        except Exception:
            with contextlib.suppress(OSError):
                path.unlink(missing_ok=True)
            if attempt < RETRIES:
                await asyncio.sleep(0.5 * attempt)
    return date, mode, None


# ============================================================
# STATS
# ============================================================

class Stats:
    __slots__ = ("ok", "miss", "fail", "done", "rows", "batches", "affected_movies")
    def __init__(self):
        self.ok = 0
        self.miss = 0
        self.fail = 0
        self.done = 0
        self.rows = 0
        self.batches = 0
        self.affected_movies: set[str] = set()


# Special queue control item: flush all pending DB writes immediately.
DB_FLUSH = object()

# ============================================================
# DB WRITER TASK
# ============================================================

async def db_writer_task(
    queue, conn, lock, loop, state, state_lock, stats,
    flush_every, flush_max_rows,
):
    pending: list = []
    pending_rows = 0

    async def flush_db():
        nonlocal pending, pending_rows
        if not pending:
            return

        batch = pending
        pending = []
        pending_rows = 0

        try:
            n = await loop.run_in_executor(
                None, _write_batch_sync, conn, lock, batch,
            )
        except Exception as e:
            log.warning(
                "DB write failed (%d jobs): %s: %s",
                len(batch), type(e).__name__, e,
            )
            for _ in batch:
                queue.task_done()
            return

        affected: set[str] = set()
        for _, _, _, variants in batch:
            for movie, _, _ in variants:
                affected.add(movie)

        async with state_lock:
            state.mark_many([(d, m) for d, m, _, _ in batch])
            state.mark_dirty(affected)
            state.save()

        stats.batches += 1
        stats.affected_movies.update(affected)

        log.debug(
            "writer batch #%d jobs=%d rows=%d affected_movies=%d",
            stats.batches, len(batch), n, len(affected),
        )

        for _ in batch:
            queue.task_done()

    while True:
        item = await queue.get()
        if item is None:
            await flush_db()
            queue.task_done()
            return

        if item is DB_FLUSH:
            await flush_db()
            queue.task_done()
            continue

        pending.append(item)
        pending_rows += len(item[2])

        if len(pending) >= flush_every or pending_rows >= flush_max_rows:
            await flush_db()



# ============================================================
# LIVE CHECKPOINT / GIT PUBLISH
# ============================================================

def git_publish_movie_files(paths: list[Path], checkpoint_no: int) -> bool:
    if not paths:
        return False

    try:
        probe = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=15,
        )
        if probe.returncode != 0:
            log.info(
                "Live checkpoint #%d: not a git worktree; kept JSONs locally.",
                checkpoint_no,
            )
            return False

        rel_paths = []
        cwd = Path.cwd().resolve()
        for p in paths:
            try:
                rel_paths.append(str(p.resolve().relative_to(cwd)))
            except ValueError:
                rel_paths.append(str(p))

        subprocess.run(
            ["git", "add", "--", *rel_paths],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=60,
        )

        diff = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=30,
        )

        if diff.returncode != 0:
            subprocess.run(
                ["git", "commit", "-m",
                 f"chore: live movie JSON checkpoint {checkpoint_no}"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=120,
            )

        # Always push. This also recovers from a previous checkpoint where
        # commit succeeded but push failed.
        subprocess.run(
            ["git", "push"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=180,
        )

        log.info(
            "Live checkpoint #%d: git push successful (%d movie JSON paths).",
            checkpoint_no,
            len(rel_paths),
        )
        return True

    except subprocess.CalledProcessError as e:
        detail = (e.stderr or e.stdout or "").strip().replace("\n", " ")
        log.warning(
            "Live checkpoint #%d git push failed: %s",
            checkpoint_no,
            detail[:500],
        )
        return False
    except Exception as e:
        log.warning(
            "Live checkpoint #%d git publish error: %s: %s",
            checkpoint_no,
            type(e).__name__,
            e,
        )
        return False


# ============================================================
# FETCH PHASE
# ============================================================

async def run_fetch_phase(
    session,
    conn,
    db_lock,
    plan,
    state,
    today,
    live_checkpoint=None,
    live_push_every=LIVE_PUSH_EVERY,
):
    loop = asyncio.get_running_loop()

    parse_pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=PARSE_THREADS,
        thread_name_prefix="parse",
    )

    db_queue: asyncio.Queue = asyncio.Queue(maxsize=DB_QUEUE_DEPTH)
    stats = Stats()
    state_lock = asyncio.Lock()

    writer = asyncio.create_task(
        db_writer_task(
            db_queue,
            conn,
            db_lock,
            loop,
            state,
            state_lock,
            stats,
            FLUSH_EVERY_N_FILES,
            FLUSH_MAX_ROWS,
        )
    )

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    total = len(plan)
    t0 = time.perf_counter()
    last_report = t0

    checkpoint_queue: asyncio.Queue = asyncio.Queue()
    checkpoint_task = None
    last_checkpoint_bucket = 0

    async def checkpoint_monitor():
        checkpoint_no = 0
        while True:
            item = await checkpoint_queue.get()
            if item is None:
                checkpoint_queue.task_done()
                return
            checkpoint_no += 1
            try:
                # Force the writer to flush its in-memory batch first.
                # DB_FLUSH is FIFO after already-queued jobs, so the following
                # join cannot deadlock behind FLUSH_EVERY_N_FILES.
                await db_queue.put(DB_FLUSH)
                await db_queue.join()
                if live_checkpoint is not None:
                    await live_checkpoint(checkpoint_no)
            except Exception as e:
                # Never kill the fetch run because of a checkpoint/push problem.
                log.warning(
                    "Live checkpoint #%d failed (non-fatal): %s: %s",
                    checkpoint_no,
                    type(e).__name__,
                    e,
                )
            finally:
                checkpoint_queue.task_done()

    async def report_progress(force=False):
        now = time.perf_counter()
        elapsed = now - t0
        rate = stats.done / elapsed if elapsed > 0 else 0.0
        eta = (total - stats.done) / rate if rate > 0 else 0.0
        log.info(
            "  progress ok=%d miss=%d fail=%d %d/%d "
            "(%.1f jobs/s, ETA %.0fs, dbq=%d/%d, rss=%.0fMB)",
            stats.ok, stats.miss, stats.fail,
            stats.done, total, rate, eta,
            db_queue.qsize(), DB_QUEUE_DEPTH, rss_mb(),
        )

    async def heartbeat():
        # Independent from worker completion. This guarantees visible output
        # even while the initial batch of HTTP downloads/parsers is still busy.
        while True:
            await asyncio.sleep(5)
            await report_progress()

    checkpoint_task = asyncio.create_task(checkpoint_monitor())
    heartbeat_task = asyncio.create_task(heartbeat())

    log.info(
        "  fetch workers started: %d tasks, concurrency=%d; "
        "heartbeat=5s",
        total, MAX_CONCURRENCY,
    )

    async def worker(date, mode, url):
        async with sem:
            path = None
            try:
                _, _, path = await download_one(session, date, mode, url)
                if not path:
                    stats.miss += 1
                    return

                rows, variants = await loop.run_in_executor(
                    parse_pool, parse_sync, path, date, mode,
                )

                await db_queue.put((date, mode, rows, variants))
                stats.ok += 1
                stats.rows += len(rows)

            except Exception as e:
                stats.fail += 1
                log.debug(
                    "[fail] %s %s: %s: %s",
                    date, mode, type(e).__name__, e,
                )
            finally:
                if path:
                    with contextlib.suppress(OSError):
                        os.remove(path)
                stats.done += 1

                if live_checkpoint is not None and live_push_every > 0:
                    bucket = stats.done // live_push_every
                    if bucket > last_checkpoint_bucket:
                        for _ in range(last_checkpoint_bucket + 1, bucket + 1):
                            await checkpoint_queue.put(True)
                        last_checkpoint_bucket = bucket


    try:
        tasks = [asyncio.create_task(worker(d, m, u)) for d, m, u in plan]
        await report_progress(force=True)
        await asyncio.gather(*tasks, return_exceptions=True)

        # Finish queued 20-job checkpoints.
        await checkpoint_queue.join()

        # Publish the tail too, even when the run ends between boundaries.
        if live_checkpoint is not None and stats.done and (
            stats.done % max(live_push_every, 1) != 0
        ):
            await checkpoint_queue.put(True)
            await checkpoint_queue.join()

    finally:
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task

        if checkpoint_task is not None:
            await checkpoint_queue.put(None)
            await checkpoint_task

        await db_queue.join()
        await db_queue.put(None)
        await writer
        parse_pool.shutdown(wait=True)
        async with state_lock:
            state.save()
        await report_progress(force=True)

    log.info(
        "Fetch complete: ok=%d miss=%d fail=%d rows=%d batches=%d "
        "affected_movies=%d in %.1fs rss=%.0fMB",
        stats.ok, stats.miss, stats.fail, stats.rows, stats.batches,
        len(stats.affected_movies), time.perf_counter() - t0, rss_mb(),
    )
    return stats


# ============================================================
# JSON WRITER — positional array metrics
# ============================================================

_J = {"ensure_ascii": False, "separators": (",", ":")}


def jdump(o: Any) -> str:
    return json.dumps(o, **_J)


def _write_comma_key(f, key: str, value_text: str, first: bool) -> bool:
    if not first:
        f.write(",")
    f.write(jdump(key))
    f.write(":")
    f.write(value_text)
    return False


def _rows_for_movie(conn: sqlite3.Connection, movie: str) -> tuple[list[tuple], list[tuple[str, str, str]]]:
    """
    One SELECT for the complete movie history. This replaces the previous
    dozens of per-dimension/per-variant SQL queries.
    """
    variants = conn.execute(
        "SELECT format, language FROM variants WHERE movie=? ORDER BY format, language",
        (movie,),
    ).fetchall()

    rows = conn.execute(
        """
        SELECT mode, dim, date, format, language, entity, state,
               gross, tickets, shows, ff, hf, seats, zero_gross
        FROM metrics
        WHERE movie=?
        ORDER BY mode, dim, format, language, entity, date
        """,
        (movie,),
    ).fetchall()

    variant_list = [(movie, f, l) for f, l in variants]
    return rows, variant_list


def build_movie(conn: sqlite3.Connection, movie: str) -> Path | None:
    """Build an ultra-compact movie JSON using shared IDs and date indexes.

    Schema:
      m  movie title
      d  dates as YYYYMMDD integers, stored once; arrays use this index
      ci city-name -> integer ID
      st state-name -> integer ID
      ch chain-name -> integer ID
      v  variants

    Variant fields:
      f/l format/language
      b daily boxoffice city rows: date -> [[city_id,state_id,metric], ...]
      a advance city rows: same structure
      k daily chain rows: date -> [[chain_id,metric], ...]
      q advance chain rows
      t daily timewise: date -> [M,A,E,N] metrics
      u advance timewise: date -> [M,A,E,N] metrics

    Metric = [gross,tickets,shows,ff,hf,seats,zero_gross].
    Occupancy and ATP are intentionally not persisted; both are derivable.
    Daily/state totals are intentionally not persisted; daily = sum cities,
    state = group cities by state_id.
    """
    raw_rows, variants = _rows_for_movie(conn, movie)
    if not variants or not raw_rows:
        return None

    slug = slugify(movie)
    if not slug:
        return None

    all_dates_str = sorted({r[2] for r in raw_rows})
    date_id = {d: i for i, d in enumerate(all_dates_str)}
    # YYYYMMDD integer is smaller than repeated ISO strings in the payload.
    all_dates = [int(d.replace("-", "")) for d in all_dates_str]

    cities: dict[str, int] = {}
    states: dict[str, int] = {}
    chains: dict[str, int] = {}

    def assign_id(mapping: dict[str, int], name: str) -> int:
        x = mapping.get(name)
        if x is None:
            x = len(mapping) + 1
            mapping[name] = x
        return x

    # Shared dictionaries across all variants/modes of this movie.
    for mode, dim, date, fmt, lang, entity, state, *_ in raw_rows:
        if dim == "c":
            city, state2 = _split_city_entity(entity, state)
            if city:
                assign_id(cities, city)
            if state2:
                assign_id(states, state2)
        elif dim == "ch" and entity:
            assign_id(chains, entity)

    variant_data: dict[tuple[str, str], dict[str, dict[int, Any]]] = {
        (fmt, lang): {
            "b": {}, "a": {}, "k": {}, "q": {}, "t": {}, "u": {}
        }
        for _, fmt, lang in variants
    }

    slot_id = {"M": 0, "A": 1, "E": 2, "N": 3}

    for (
        mode, dim, date, fmt, lang, entity, state,
        gross, tickets, shows, ff, hf, seats, zero_gross
    ) in raw_rows:
        key = (fmt, lang)
        data = variant_data.setdefault(
            key, {"b": {}, "a": {}, "k": {}, "q": {}, "t": {}, "u": {}}
        )
        di = date_id[date]
        metric = [
            round(gross, 2), int(tickets), int(shows),
            int(ff), int(hf), int(seats), int(zero_gross)
        ]

        if dim == "c":
            city, state2 = _split_city_entity(entity, state)
            if not city:
                continue
            ci = cities[city]
            si = states.get(state2, 0) if state2 else 0
            bucket = data["b" if mode == "daily" else "a"]
            bucket.setdefault(di, {})[(ci, si)] = metric

        elif dim == "ch":
            if entity:
                chi = chains[entity]
                bucket = data["k" if mode == "daily" else "q"]
                bucket.setdefault(di, {})[chi] = metric

        elif dim == "tm":
            if entity in slot_id:
                bucket = data["t" if mode == "daily" else "u"]
                bucket.setdefault(di, {})[slot_id[entity]] = metric

        # dim == d deliberately ignored: daily totals are derived from cities.

    def render_city(bucket: dict[int, dict[tuple[int, int], list]]) -> list:
        out = [[] for _ in all_dates]
        for di, rows in bucket.items():
            out[di] = [
                [ci, si, metric]
                for (ci, si), metric in sorted(rows.items())
            ]
        return out

    def render_chain(bucket: dict[int, dict[int, list]]) -> list:
        out = [[] for _ in all_dates]
        for di, rows in bucket.items():
            out[di] = [[chi, metric] for chi, metric in sorted(rows.items())]
        return out

    def render_time(bucket: dict[int, dict[int, list]]) -> list:
        out = [None] * len(all_dates)
        for di, rows in bucket.items():
            slots = [None, None, None, None]
            for si, metric in rows.items():
                if 0 <= si < 4:
                    slots[si] = metric
            out[di] = slots
        return out

    versions = []
    seen_variants = set()
    for _, fmt, lang in variants:
        key = (fmt, lang)
        if key in seen_variants:
            continue
        seen_variants.add(key)
        data = variant_data[key]
        v: dict[str, Any] = {}
        if fmt:
            v["f"] = fmt
        if lang:
            v["l"] = lang
        if data["b"]:
            v["b"] = render_city(data["b"])
        if data["a"]:
            v["a"] = render_city(data["a"])
        if data["k"]:
            v["k"] = render_chain(data["k"])
        if data["q"]:
            v["q"] = render_chain(data["q"])
        if data["t"]:
            v["t"] = render_time(data["t"])
        if data["u"]:
            v["u"] = render_time(data["u"])
        versions.append(v)

    payload: dict[str, Any] = {
        "m": movie,
        "d": all_dates,
        "v": versions,
    }
    if cities:
        payload["ci"] = {name: i for name, i in sorted(cities.items(), key=lambda x: x[1])}
    if states:
        payload["st"] = {name: i for name, i in sorted(states.items(), key=lambda x: x[1])}
    if chains:
        payload["ch"] = {name: i for name, i in sorted(chains.items(), key=lambda x: x[1])}

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / f"{slug}.json"
    tmp = OUTPUT_DIR / f"{slug}.json.tmp.{os.getpid()}.{threading.get_ident()}"
    try:
        with tmp.open("w", encoding="utf-8", newline="\n", buffering=FILE_WRITE_BUFFER) as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, out)
    finally:
        with contextlib.suppress(OSError):
            tmp.unlink()
    return out

def generate_jobs(state: State, today: dt.date, force_full: bool):
    """
    Returns (jobs, mode) where mode ∈ {"full", "incremental"}.

      FULL:        every daily date from START_DATE → today plus every
                   advance date from 2026-01-01 → today+ADVANCE_FUTURE_DAYS.
                   Caller filters by state so only missing days are fetched.

      INCREMENTAL: last INCREMENTAL_PAST_DAYS daily dates, plus every
                   day from today → today+ADVANCE_FUTURE_DAYS in both
                   daily and advance mode. Caller does NOT filter by
                   state — recent data can be revised by the source.
    """
    if not force_full and is_historical_complete(state, today):
        jobs: list[tuple[dt.date, str, str]] = []

        # Recent past — daily only
        for i in range(INCREMENTAL_PAST_DAYS, 0, -1):
            d = today - dt.timedelta(days=i)
            u = source_url(d, "daily")
            if u:
                jobs.append((d, "daily", u))

        # Today + future — both daily and advance
        end = today + dt.timedelta(days=ADVANCE_FUTURE_DAYS)
        d = today
        while d <= end:
            for mode in ("daily", "advance"):
                u = source_url(d, mode)
                if u:
                    jobs.append((d, mode, u))
            d += dt.timedelta(days=1)

        return jobs, "incremental"

    # FULL
    jobs = []
    d = START_DATE
    while d <= today:
        u = source_url(d, "daily")
        if u:
            jobs.append((d, "daily", u))
        d += dt.timedelta(days=1)

    d = dt.date(2026, 1, 1)
    end = today + dt.timedelta(days=ADVANCE_FUTURE_DAYS)
    while d <= end:
        u = source_url(d, "advance")
        if u:
            jobs.append((d, "advance", u))
        d += dt.timedelta(days=1)

    return jobs, "full"


def should_fetch(date, mode, state, today) -> bool:
    """Full-mode filter: skip anything already in state (except current month)."""
    if date >= today.replace(day=1):
        return True
    return not state.has(date, mode)


# ============================================================
# MONTH STATUS
# ============================================================

def report_month_status(plan, state, today) -> None:
    months = defaultdict(lambda: {"daily": [0, 0], "advance": [0, 0]})
    for d, mode, _ in plan:
        k = f"{d.year:04d}-{d.month:02d}"
        months[k][mode][0] += 1
        if state.has(d, mode):
            months[k][mode][1] += 1

    current_key = f"{today.year:04d}-{today.month:02d}"
    log.info("")
    log.info("================================================")
    log.info(" MONTH STATUS (state.json)")
    log.info("================================================")
    incomplete = []
    for month in sorted(months):
        parts = []
        for mode in ("daily", "advance"):
            p, dn = months[month][mode]
            if p == 0:
                parts.append(f"{mode}=n/a")
            elif dn >= p:
                parts.append(f"{mode}=done({dn}/{p})")
            elif dn == 0:
                parts.append(f"{mode}=missing(0/{p})")
                if mode == "daily" and month < current_key:
                    incomplete.append((month, mode, dn, p))
            else:
                parts.append(f"{mode}=partial({dn}/{p})")
                if mode == "daily" and month < current_key:
                    incomplete.append((month, mode, dn, p))
        tag = "  <- current" if month == current_key else ""
        log.info("  %s  %s%s", month, "  ".join(parts), tag)
    log.info("")
    if incomplete:
        log.info("INCOMPLETE PAST MONTHS:")
        for month, mode, dn, p in incomplete:
            log.info("  !! %s  %s  %d/%d", month, mode, dn, p)
    else:
        log.info("All past months are complete.")


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="BFILMY builder")
    p.add_argument("--fresh", action="store_true",
                   help="Wipe state, DB, existing movie JSONs, then full run.")
    p.add_argument("--rebuild", action="store_true",
                   help="Skip fetch; rebuild all movie JSONs from DB.")
    p.add_argument("--force-full", action="store_true",
                   help="Ignore state; treat as full historical fetch.")
    p.add_argument("--refetch-month", metavar="YYYY-MM",
                   help="Clear that month from state and DB, then refetch.")
    p.add_argument("--concurrency", type=int, default=MAX_CONCURRENCY)
    p.add_argument("--parse-workers", type=int, default=PARSE_THREADS)
    p.add_argument("--flush-files", type=int, default=FLUSH_EVERY_N_FILES)
    p.add_argument("--flush-rows", type=int, default=FLUSH_MAX_ROWS)
    p.add_argument("--queue-depth", type=int, default=DB_QUEUE_DEPTH)
    p.add_argument(
        "--push-every",
        type=int,
        default=LIVE_PUSH_EVERY,
        help="Build/push dirty movie JSONs after every N completed fetch jobs; 0 disables live publishing.",
    )
    p.add_argument("--low-memory", action="store_true",
                   help="Ultra-low-memory preset (Replit free tier).")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


# ============================================================
# MAIN
# ============================================================

async def amain(args: argparse.Namespace) -> None:
    global MAX_CONCURRENCY, PARSE_THREADS, DB_QUEUE_DEPTH
    global FLUSH_EVERY_N_FILES, FLUSH_MAX_ROWS, LIVE_PUSH_EVERY

    if args.low_memory:
        MAX_CONCURRENCY = 6
        PARSE_THREADS = 1
        DB_QUEUE_DEPTH = 3
        FLUSH_EVERY_N_FILES = 10
        FLUSH_MAX_ROWS = 10_000
        LIVE_PUSH_EVERY = max(0, args.push_every)
    else:
        MAX_CONCURRENCY = args.concurrency
        PARSE_THREADS = args.parse_workers
        DB_QUEUE_DEPTH = args.queue_depth
        FLUSH_EVERY_N_FILES = args.flush_files
        FLUSH_MAX_ROWS = args.flush_rows
        LIVE_PUSH_EVERY = max(0, args.push_every)

    started = time.perf_counter()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today = today_ist()

    log.info("")
    log.info("================================================")
    log.info(" BFILMY MOVIE JSON GENERATOR")
    log.info("================================================")
    log.info("Today IST       : %s", today)
    log.info("Concurrency     : %d downloads", MAX_CONCURRENCY)
    log.info("Parse threads   : %d", PARSE_THREADS)
    log.info("Flush policy    : %d files OR %d rows",
             FLUSH_EVERY_N_FILES, FLUSH_MAX_ROWS)
    log.info("Incremental past: %d days", INCREMENTAL_PAST_DAYS)
    log.info("Live push every: %d fetches", LIVE_PUSH_EVERY)
    log.info("uvloop          : %s", _HAS_UVLOOP)
    log.info("ijson backend   : %s", detect_ijson_backend())
    log.info("RSS start       : %.0f MB", rss_mb())

    if args.fresh:
        log.warning("--fresh: wiping state, DB, existing movie JSONs")
        with contextlib.suppress(Exception):
            STATE_PATH.unlink(missing_ok=True)
        for suffix in ("", "-wal", "-shm"):
            f = DB_PATH.with_name(DB_PATH.name + suffix)
            with contextlib.suppress(Exception):
                f.unlink(missing_ok=True)
        for f in OUTPUT_DIR.glob("*.json"):
            with contextlib.suppress(Exception):
                f.unlink()
        for f in OUTPUT_DIR.glob("*.json.tmp"):
            with contextlib.suppress(Exception):
                f.unlink()

    state = State(STATE_PATH)
    state.load()
    log.info("Days in state   : %d", len(state.days))

    if args.refetch_month:
        try:
            y, m = args.refetch_month.split("-")
            year, month = int(y), int(m)
        except Exception:
            log.error("Invalid --refetch-month; use YYYY-MM")
            sys.exit(2)
        removed = state.clear_month(year, month)
        log.warning("--refetch-month %04d-%02d: cleared %d state entries",
                    year, month, removed)
        conn0 = open_database(DB_PATH)
        try:
            cur = conn0.execute("DELETE FROM metrics WHERE date LIKE ?",
                                (f"{year:04d}-{month:02d}-%",))
            log.warning("--refetch-month: removed %d metric rows", cur.rowcount)
        finally:
            conn0.commit()
            conn0.close()

    # ---- Decide run mode ---------------------------------------------
    plan_all, mode = generate_jobs(state, today, args.force_full)
    log.info("Run mode        : %s", mode.upper())
    log.info("Planned jobs    : %d", len(plan_all))

    report_month_status(plan_all, state, today)

    conn = open_database(DB_PATH)
    db_lock = threading.RLock()

    try:
        if not args.rebuild:
            if mode == "full":
                # Skip days already in state (except current month).
                pending = [j for j in plan_all
                           if should_fetch(j[0], j[1], state, today)]
            else:
                # Incremental — always fetch, data may have been revised.
                pending = plan_all

            log.info("")
            log.info("================================================")
            log.info(" FETCH PHASE  [%s]", mode)
            log.info("================================================")
            log.info("Pending jobs    : %d of %d", len(pending), len(plan_all))

            if pending:
                connector = aiohttp.TCPConnector(
                    limit=MAX_CONCURRENCY,
                    limit_per_host=MAX_CONCURRENCY,
                    ttl_dns_cache=600,
                    keepalive_timeout=90,
                    enable_cleanup_closed=True,
                )
                timeout = aiohttp.ClientTimeout(
                    total=REQUEST_TIMEOUT,
                    connect=CONNECT_TIMEOUT,
                    sock_read=REQUEST_TIMEOUT,
                )
                headers = {
                    "User-Agent": "Mozilla/5.0 (BFILMY GitHub Actions)",
                    "Accept": "application/json,text/plain,*/*",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                }
                async with aiohttp.ClientSession(
                    connector=connector, timeout=timeout, headers=headers,
                ) as session:
                    try:
                        async def live_checkpoint(checkpoint_no):
                            # Only movies currently marked dirty are rebuilt.
                            # Successful rebuilds clear the dirty bit, so a movie
                            # changed again later will be rebuilt at a later checkpoint.
                            with db_lock:
                                dirty_movies = sorted(state.dirty)

                            if not dirty_movies:
                                log.info(
                                    "Live checkpoint #%d: nothing dirty.",
                                    checkpoint_no,
                                )
                                return

                            thread_local_live = threading.local()

                            def get_live_read_conn():
                                c = getattr(thread_local_live, "conn", None)
                                if c is None:
                                    c = sqlite3.connect(
                                        DB_PATH,
                                        timeout=180,
                                        check_same_thread=True,
                                    )
                                    c.execute("PRAGMA journal_mode=WAL")
                                    c.execute("PRAGMA synchronous=NORMAL")
                                    c.execute("PRAGMA busy_timeout=180000")
                                    c.execute("PRAGMA cache_size=-32768")
                                    c.execute("PRAGMA mmap_size=268435456")
                                    thread_local_live.conn = c
                                return c

                            def build_live_one(movie_name):
                                try:
                                    out = build_movie(
                                        get_live_read_conn(),
                                        movie_name,
                                    )
                                    return movie_name, out, None
                                except Exception as e:
                                    return (
                                        movie_name,
                                        None,
                                        f"{type(e).__name__}: {e}",
                                    )

                            built_paths = []
                            failed = set()
                            t_live = time.perf_counter()

                            with concurrent.futures.ThreadPoolExecutor(
                                max_workers=max(
                                    1,
                                    min(LIVE_BUILD_WORKERS, os.cpu_count() or 2),
                                ),
                                thread_name_prefix=f"live-json-{checkpoint_no}",
                            ) as pool:
                                futures = [
                                    pool.submit(build_live_one, movie)
                                    for movie in dirty_movies
                                ]
                                for fut in concurrent.futures.as_completed(futures):
                                    movie_name, out, err = fut.result()
                                    if out is not None:
                                        built_paths.append(out)
                                    else:
                                        failed.add(movie_name)
                                        log.warning(
                                            "[live movie error] %s: %s",
                                            movie_name,
                                            err,
                                        )

                            successful = set(dirty_movies) - failed

                            push_ok = True
                            if built_paths and os.getenv("GITHUB_ACTIONS", "").lower() == "true":
                                push_ok = await loop.run_in_executor(
                                    None,
                                    git_publish_movie_files,
                                    built_paths,
                                    checkpoint_no,
                                )

                            # On GitHub, only clear dirty after the checkpoint
                            # is actually pushed. A failed push therefore gets
                            # retried at the next checkpoint/run.
                            if push_ok:
                                state.clear_dirty(successful)
                            else:
                                state.mark_dirty(successful)

                            if failed:
                                state.mark_dirty(failed)

                            state.save()

                            log.info(
                                "Live checkpoint #%d: dirty=%d built=%d failed=%d pushed=%s in %.1fs rss=%.0fMB",
                                checkpoint_no,
                                len(dirty_movies),
                                len(built_paths),
                                len(failed),
                                push_ok,
                                time.perf_counter() - t_live,
                                rss_mb(),
                            )

                        stats = await run_fetch_phase(
                            session,
                            conn,
                            db_lock,
                            pending,
                            state,
                            today,
                            live_checkpoint=live_checkpoint,
                            live_push_every=LIVE_PUSH_EVERY,
                        )
                    except asyncio.CancelledError:
                        log.warning("Fetch cancelled")
                        state.save()
                        raise
            else:
                log.info("Nothing to fetch.")
        else:
            log.info("--rebuild: skipping fetch phase")

        with db_lock:
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchall()

            t = time.perf_counter()
            log.info("Running ANALYZE…")
            try:
                conn.execute("ANALYZE")
            except sqlite3.OperationalError as e:
                log.warning("ANALYZE failed (non-fatal): %s", e)
            log.info("ANALYZE finished in %.1fs", time.perf_counter() - t)

        # ---- FINAL BUILD PHASE -------------------------------------
        # Rebuild only dirty movies. In --rebuild, rebuild every known movie.
        # Dirty state makes this crash-safe: DB/state can survive an interrupted
        # run without requiring a refetch just to regenerate movie JSONs.
        with db_lock:
            all_movies = [
                r[0] for r in conn.execute(
                    "SELECT movie FROM variants GROUP BY movie ORDER BY movie"
                ).fetchall()
            ]

        if args.rebuild:
            to_build = all_movies
        else:
            # State dirty set is authoritative; also include movies affected
            # by this run in case the in-memory fetch phase is newer than a
            # persisted state save.
            to_build = sorted(set(state.dirty) | set(
                getattr(locals().get("stats", None), "affected_movies", set())
                if "stats" in locals() else set()
            ))

        log.info("")
        log.info("================================================")
        log.info(" FINAL BUILD PHASE")
        log.info("================================================")
        log.info(
            "Movies total=%d dirty=%d to_build=%d",
            len(all_movies), len(state.dirty), len(to_build),
        )

        build_workers = max(1, min(PARSE_THREADS, os.cpu_count() or 2))
        built = 0
        failed_builds: set[str] = set()
        tb = time.perf_counter()

        thread_local = threading.local()

        def get_read_conn() -> sqlite3.Connection:
            c = getattr(thread_local, "conn", None)
            if c is None:
                c = sqlite3.connect(
                    DB_PATH,
                    timeout=180,
                    check_same_thread=True,
                )
                c.execute("PRAGMA journal_mode=WAL")
                c.execute("PRAGMA synchronous=NORMAL")
                c.execute("PRAGMA busy_timeout=180000")
                c.execute("PRAGMA cache_size=-65536")
                c.execute("PRAGMA mmap_size=536870912")
                thread_local.conn = c
            return c

        def build_one(movie_name: str) -> tuple[str, bool, str | None]:
            try:
                out = build_movie(get_read_conn(), movie_name)
                return movie_name, bool(out), None
            except Exception as e:
                return movie_name, False, f"{type(e).__name__}: {e}"

        if to_build:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=build_workers,
                thread_name_prefix="json",
            ) as pool:
                futures = [pool.submit(build_one, movie_name) for movie_name in to_build]
                for i, fut in enumerate(
                    concurrent.futures.as_completed(futures), 1
                ):
                    movie_name, ok, err = fut.result()
                    if ok:
                        built += 1
                    else:
                        failed_builds.add(movie_name)
                        log.warning("[movie error] %s: %s", movie_name, err)

                    if i % 200 == 0 or i == len(futures):
                        elapsed = time.perf_counter() - tb
                        rate = i / elapsed if elapsed > 0 else 0.0
                        eta = (len(futures) - i) / rate if rate > 0 else 0.0
                        log.info(
                            "Final build: %d/%d (%.1f/s, ETA %.0fs, rss=%.0fMB)",
                            i, len(futures), rate, eta, rss_mb(),
                        )

        successful_dirty = set(to_build) - failed_builds
        state.clear_dirty(successful_dirty)
        if failed_builds:
            state.mark_dirty(failed_builds)

        state.save()
        with db_lock:
            conn.commit()

        elapsed = time.perf_counter() - started
        log.info("")
        log.info("================================================")
        log.info(" DONE")
        log.info("================================================")
        log.info("Run mode                  : %s", mode)
        log.info("Movies rebuilt             : %d", built)
        log.info("Movies still dirty         : %d", len(state.dirty))
        log.info("Time                      : %.2fs", elapsed)
        log.info("RSS final                 : %.0f MB", rss_mb())
        log.info("Output dir                : %s", OUTPUT_DIR.resolve())
        log.info("")
    finally:
        conn.close()


def main() -> None:
    args = parse_args()
    setup_logging(verbose=args.verbose)

    with contextlib.suppress(Exception):
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)

    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        log.warning("Interrupted by user")


if __name__ == "__main__":
    main()
