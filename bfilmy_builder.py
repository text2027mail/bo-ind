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
MAX_CONCURRENCY = 40
PARSE_THREADS = 4
DB_QUEUE_DEPTH = 12

FLUSH_EVERY_N_FILES = 30
FLUSH_MAX_ROWS = 50_000
JSON_REBUILD_EVERY_N_BATCHES = 10

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
    if value is None:
        return None
    text = str(value).strip().upper()
    hour: int | None = None
    for fmt in ("%I:%M %p", "%H:%M", "%I %p"):
        try:
            hour = dt.datetime.strptime(text, fmt).hour
            break
        except ValueError:
            pass
    if hour is None:
        return None
    if 5 <= hour < 12:
        return "M"
    if 12 <= hour < 17:
        return "A"
    if 17 <= hour < 21:
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
        except Exception as e:
            log.warning("Could not load state (%s); starting fresh", e)
            self.days = {}

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
    today = today_ist()

    if d.year <= 2025:
        if mode != "daily":
            return None
        return (
            "https://bfilmyapi2025.pages.dev/"
            f"daily/data/{d.year}/{d:%m-%d}_finalsummary.json"
        )

    if d.year == 2026 and d.month == 1:
        if mode == "daily":
            return (
                "https://bfilmyapi2026.pages.dev/"
                f"daily/data/2026/{d:%m-%d}_finalsummary.json"
            )
        if mode == "advance":
            return (
                "https://bfilmyapi2026.pages.dev/"
                f"advance/data/2026/{d:%m-%d}_finalsummary.json"
            )

    if d >= today - dt.timedelta(days=1):
        return (
            "https://bfilmyapi.pages.dev/"
            f"{mode}/data/{d:%Y%m%d}/finaldetailed.json"
        )

    return (
        "https://bfilmyapi2026.pages.dev/"
        f"{mode}/data/2026/{d:%m-%d}_finaldetailed.json"
    )


# ============================================================
# NUMBER HELPERS
# ============================================================

def to_float(v: Any) -> float:
    try:
        return 0.0 if v in (None, "") else float(v)
    except Exception:
        return 0.0


def to_int(v: Any) -> int:
    try:
        return 0 if v in (None, "") else int(float(v))
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

def explicit_flag(row: dict, flag: str) -> bool:
    if flag == "ff":
        keys = ("ff", "fastfilling", "fast_filling", "fastFilling", "fast-filling")
        status = {"ff", "fastfilling", "fast filling", "fast-filling"}
    else:
        keys = ("hf", "housefull", "house_full", "houseFull", "house-full")
        status = {"hf", "housefull", "house full", "house-full"}

    for k in keys:
        if k not in row:
            continue
        v = row[k]
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return v > 0
        if str(v).strip().lower() in status:
            return True

    for k in ("status", "booking_status", "bookingStatus", "booking"):
        if k in row and str(row[k]).strip().lower() in status:
            return True
    return False


def get_ff(row: dict) -> int:
    return int(explicit_flag(row, "ff"))


def get_hf(row: dict, sold: int, seats: int) -> int:
    if explicit_flag(row, "hf"):
        return 1
    available = row.get("available")
    if seats > 0 and available is not None and to_int(available) == 0 and sold >= seats:
        return 1
    return 0


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
    return [
        round(g, 2),
        occupancy(t, se),
        int(sh),
        int(ff),
        int(hf),
        int(t),
        int(se),
        int(z),
    ]


def write_metric(f, v) -> None:
    f.write(json.dumps(compact_metric(v), **_J))


def compact_timewise(g, t, sh, ff, hf, se) -> list:
    return [
        int(sh),
        int(ff),
        int(hf),
        int(t),
        round(g, 2),
        occupancy(t, se),
        atp(g, t),
    ]


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
    if ijson is not None:
        with open(path, "rb") as f:
            yield from ijson.items(f, "data.item")
        return
    with open(path, "r", encoding="utf-8") as f:
        for row in json.load(f).get("data", []):
            yield row


def _iter_summary(path: str) -> Iterator[tuple[str, dict]]:
    if ijson is not None:
        with open(path, "rb") as f:
            yield from ijson.kvitems(f, "movies")
        return
    with open(path, "r", encoding="utf-8") as f:
        for k, v in json.load(f).get("movies", {}).items():
            yield k, v


def parse_detailed_sync(path: str, date: dt.date) -> tuple[list, list]:
    date_str = date.isoformat()
    include_timewise = date >= dt.date(2026, 2, 1)
    buckets: dict[tuple, dict] = {}

    for row in _iter_detailed(path):
        if not isinstance(row, dict):
            continue
        raw_movie = row.get("movie")
        if not raw_movie:
            continue

        movie, fmt, lang = parse_movie_variant(raw_movie)
        key = (movie, fmt, lang)
        bucket = buckets.get(key)
        if bucket is None:
            bucket = {
                "daily": metric_zero(),
                "cities": {},
                "chains": {},
                "times": {s: metric_zero() for s in "MAEN"},
            }
            buckets[key] = bucket

        gross = to_float(row.get("gross"))
        tickets = to_int(row.get("sold"))
        seats = to_int(row.get("totalSeats"))
        ff = get_ff(row)
        hf = get_hf(row, tickets, seats)
        zero = int(gross == 0)

        add_metric(bucket["daily"], gross, tickets, 1, ff, hf, seats, zero)

        city = str(row.get("city") or "").strip()
        state = str(row.get("state") or "").strip()
        if city:
            cd = bucket["cities"].get(city)
            if cd is None:
                cd = {"state": state, "metric": metric_zero()}
                bucket["cities"][city] = cd
            elif not cd["state"] and state:
                cd["state"] = state
            add_metric(cd["metric"], gross, tickets, 1, ff, hf, seats, zero)

        chain = str(row.get("chain") or "").strip()
        if chain:
            ch = bucket["chains"].get(chain)
            if ch is None:
                ch = metric_zero()
                bucket["chains"][chain] = ch
            add_metric(ch, gross, tickets, 1, ff, hf, seats, zero)

        if include_timewise:
            slot = get_time_slot(row.get("time"))
            if slot:
                add_metric(bucket["times"][slot], gross, tickets, 1, ff, hf, seats, zero)

    rows: list[tuple] = []
    variants: list[tuple[str, str, str]] = []

    for (movie, fmt, lang), b in buckets.items():
        variants.append((movie, fmt, lang))

        rows.append(("__MODE__", "d", date_str, movie, fmt, lang, "", "", *b["daily"]))
        for city, cd in b["cities"].items():
            rows.append(("__MODE__", "c", date_str, movie, fmt, lang,
                         city, cd["state"], *cd["metric"]))
        for chain, vals in b["chains"].items():
            rows.append(("__MODE__", "ch", date_str, movie, fmt, lang,
                         chain, "", *vals))
        if include_timewise:
            for slot in "MAEN":
                vals = b["times"][slot]
                if vals[2]:
                    rows.append(("__MODE__", "tm", date_str, movie, fmt, lang,
                                 slot, "", *vals))

    return rows, variants


def parse_summary_sync(path: str, date: dt.date) -> tuple[list, list]:
    date_str = date.isoformat()
    rows: list[tuple] = []
    variants: list[tuple[str, str, str]] = []

    for raw_movie, data in _iter_summary(path):
        if not isinstance(data, dict):
            continue
        movie, fmt, lang = parse_movie_variant(raw_movie)
        variants.append((movie, fmt, lang))

        gross = to_float(data.get("gross"))
        tickets = to_int(data.get("sold"))
        shows = to_int(data.get("shows"))
        seats = to_int(data.get("totalSeats"))
        ff = to_int(data.get("fastfilling"))
        hf = to_int(data.get("housefull"))

        rows.append(("daily", "d", date_str, movie, fmt, lang, "", "",
                     gross, tickets, shows, ff, hf, seats, 0))

        for item in (data.get("details") or []):
            if not isinstance(item, dict):
                continue
            city = str(item.get("city") or "").strip()
            if not city:
                continue
            rows.append((
                "daily", "c", date_str, movie, fmt, lang,
                city, str(item.get("state") or ""),
                to_float(item.get("gross")),
                to_int(item.get("sold")),
                to_int(item.get("shows")),
                to_int(item.get("fastfilling")),
                to_int(item.get("housefull")),
                to_int(item.get("totalSeats")),
                0,
            ))

        for item in (data.get("Chain_details") or []):
            if not isinstance(item, dict):
                continue
            chain = str(item.get("chain") or "").strip()
            if not chain:
                continue
            rows.append((
                "daily", "ch", date_str, movie, fmt, lang, chain, "",
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
    for i, r in enumerate(rows):
        if r[0] == "__MODE__":
            rows[i] = (mode,) + r[1:]


def parse_sync(path: str, date: dt.date, mode: str) -> tuple[list, list]:
    if mode == "daily" and date <= dt.date(2026, 1, 31):
        return parse_summary_sync(path, date)
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
    __slots__ = ("ok", "miss", "fail", "done", "rows", "batches", "built_movies")
    def __init__(self):
        self.ok = 0
        self.miss = 0
        self.fail = 0
        self.done = 0
        self.rows = 0
        self.batches = 0
        self.built_movies: set[str] = set()


# ============================================================
# DB WRITER TASK
# ============================================================

async def db_writer_task(
    queue, json_queue, conn, lock, loop,
    state, state_lock, stats,
    flush_every, flush_max_rows, json_rebuild_every,
):
    pending: list = []
    pending_rows = 0
    batches_since_rebuild = 0

    async def flush_db():
        nonlocal pending, pending_rows
        if not pending:
            return None
        batch = pending
        pending = []
        pending_rows = 0

        try:
            n = await loop.run_in_executor(
                None, _write_batch_sync, conn, lock, batch,
            )
        except Exception as e:
            log.warning("DB write failed (%d days): %s: %s",
                        len(batch), type(e).__name__, e)
            for _ in batch:
                queue.task_done()
            return None

        async with state_lock:
            state.mark_many([(d, m) for d, m, _, _ in batch])
            state.save()

        stats.batches += 1

        affected: set[str] = set()
        for _, _, _, variants in batch:
            for v in variants:
                affected.add(v[0])

        with lock:
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute("PRAGMA shrink_memory")
        gc.collect()

        log.debug("writer flushed batch #%d days=%d rows=%d affected_movies=%d",
                  stats.batches, len(batch), n, len(affected))

        for _ in batch:
            queue.task_done()
        return affected

    async def maybe_enqueue_json(movies, force=False):
        nonlocal batches_since_rebuild
        if not movies:
            return
        if not force and batches_since_rebuild < json_rebuild_every:
            return
        batches_since_rebuild = 0
        for m in movies:
            json_queue.put_nowait(m)

    while True:
        item = await queue.get()

        if item is None:
            affected = await flush_db()
            if affected:
                await maybe_enqueue_json(affected, force=True)
            json_queue.put_nowait(None)
            queue.task_done()
            return

        pending.append(item)
        pending_rows += len(item[2])

        if (len(pending) >= flush_every
                or pending_rows >= flush_max_rows):
            affected = await flush_db()
            if affected:
                batches_since_rebuild += 1
                await maybe_enqueue_json(affected)


# ============================================================
# JSON REBUILD TASK
# ============================================================

def _rebuild_movie_sync(conn, lock, movie):
    with lock:
        return build_movie(conn, movie)


async def json_rebuild_task(json_queue, conn, lock, loop, stats):
    rebuilt = 0
    while True:
        movie = await json_queue.get()
        if movie is None:
            json_queue.task_done()
            log.debug("JSON rebuild task done (%d movies)", rebuilt)
            return
        try:
            result = await loop.run_in_executor(
                None, _rebuild_movie_sync, conn, lock, movie,
            )
            if result is not None:
                stats.built_movies.add(movie)
                rebuilt += 1
        except Exception as e:
            log.warning("JSON rebuild failed for %s: %s: %s",
                        movie, type(e).__name__, e)
        finally:
            json_queue.task_done()


# ============================================================
# FETCH PHASE
# ============================================================

async def run_fetch_phase(session, conn, db_lock, plan, state, today):
    loop = asyncio.get_running_loop()
    parse_pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=PARSE_THREADS, thread_name_prefix="parse",
    )

    db_queue: asyncio.Queue = asyncio.Queue(maxsize=DB_QUEUE_DEPTH)
    json_queue: asyncio.Queue = asyncio.Queue()

    stats = Stats()
    state_lock = asyncio.Lock()

    writer = asyncio.create_task(
        db_writer_task(
            db_queue, json_queue, conn, db_lock, loop,
            state, state_lock, stats,
            FLUSH_EVERY_N_FILES, FLUSH_MAX_ROWS,
            JSON_REBUILD_EVERY_N_BATCHES,
        )
    )
    json_task = asyncio.create_task(
        json_rebuild_task(json_queue, conn, db_lock, loop, stats)
    )

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    total = len(plan)
    t0 = time.perf_counter()
    last_report = t0

    async def report_throttled():
        nonlocal last_report
        now = time.perf_counter()
        if now - last_report >= 5.0:
            elapsed = now - t0
            rate = stats.done / elapsed if elapsed > 0 else 0.0
            eta = (total - stats.done) / rate if rate > 0 else 0.0
            log.info(
                "  progress  ok=%d miss=%d fail=%d  %d/%d  "
                "(%.1f/s, ETA %.0fs, dbq=%d/%d, jsonq=%d, "
                "json_done=%d, rss=%.0fMB)",
                stats.ok, stats.miss, stats.fail,
                stats.done, total, rate, eta,
                db_queue.qsize(), DB_QUEUE_DEPTH,
                json_queue.qsize(), len(stats.built_movies), rss_mb(),
            )
            last_report = now

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
                log.debug("[fail] %s %s: %s: %s",
                          date, mode, type(e).__name__, e)
            finally:
                if path:
                    with contextlib.suppress(OSError):
                        os.remove(path)
                stats.done += 1
                await report_throttled()

    try:
        tasks = [asyncio.create_task(worker(d, m, u)) for d, m, u in plan]
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await db_queue.join()
        await db_queue.put(None)
        await writer
        await json_queue.join()
        await json_task
        parse_pool.shutdown(wait=True)
        async with state_lock:
            state.save()

    log.info(
        "Fetch phase complete: ok=%d miss=%d fail=%d rows=%d batches=%d "
        "json_built=%d in %.1fs  rss=%.0fMB",
        stats.ok, stats.miss, stats.fail, stats.rows, stats.batches,
        len(stats.built_movies),
        time.perf_counter() - t0, rss_mb(),
    )
    return stats


# ============================================================
# JSON WRITER — positional array metrics
# ============================================================

_J = {"ensure_ascii": False, "separators": (",", ":")}


def jdump(o: Any) -> str:
    return json.dumps(o, **_J)


def _has_rows(conn, movie, fmt, lang, mode, dim) -> bool:
    return conn.execute(
        "SELECT 1 FROM metrics WHERE movie=? AND format=? AND language=? "
        "AND mode=? AND dim=? LIMIT 1",
        (movie, fmt, lang, mode, dim),
    ).fetchone() is not None


def _write_daily_map(f, conn, movie, fmt, lang, mode) -> None:
    cur = conn.execute(
        "SELECT date, gross, tickets, shows, ff, hf, seats, zero_gross "
        "FROM metrics WHERE movie=? AND format=? AND language=? AND mode=? "
        "AND dim='d' ORDER BY date",
        (movie, fmt, lang, mode),
    )
    first = True
    for date, g, t, sh, ff, hf, se, z in cur:
        if not first:
            f.write(",")
        first = False
        f.write(jdump(date))
        f.write(":")
        write_metric(f, (g, t, sh, ff, hf, se, z))


def _write_citywise(f, conn, movie, fmt, lang, mode) -> None:
    cur = conn.execute(
        "SELECT entity, state, date, gross, tickets, shows, ff, hf, seats, "
        "zero_gross FROM metrics WHERE movie=? AND format=? AND language=? "
        "AND mode=? AND dim='c' ORDER BY entity, date",
        (movie, fmt, lang, mode),
    )
    cur_city = None
    dfirst = True
    cfirst = True
    for city, state, date, g, t, sh, ff, hf, se, z in cur:
        if city != cur_city:
            if cur_city is not None:
                f.write("}}")
            if not cfirst:
                f.write(",")
            cfirst = False
            f.write(jdump(city))
            f.write(":{")
            if state:
                f.write('"s":')
                f.write(jdump(state))
                f.write(",")
            f.write('"d":{')
            cur_city = city
            dfirst = True
        if not dfirst:
            f.write(",")
        dfirst = False
        f.write(jdump(date))
        f.write(":")
        write_metric(f, (g, t, sh, ff, hf, se, z))
    if cur_city is not None:
        f.write("}}")


def _write_chainwise(f, conn, movie, fmt, lang, mode) -> None:
    cur = conn.execute(
        "SELECT entity, date, gross, tickets, shows, ff, hf, seats, "
        "zero_gross FROM metrics WHERE movie=? AND format=? AND language=? "
        "AND mode=? AND dim='ch' ORDER BY entity, date",
        (movie, fmt, lang, mode),
    )
    cur_ch = None
    dfirst = True
    cfirst = True
    for ch, date, g, t, sh, ff, hf, se, z in cur:
        if ch != cur_ch:
            if cur_ch is not None:
                f.write("}")
            if not cfirst:
                f.write(",")
            cfirst = False
            f.write(jdump(ch))
            f.write(":{")
            cur_ch = ch
            dfirst = True
        if not dfirst:
            f.write(",")
        dfirst = False
        f.write(jdump(date))
        f.write(":")
        write_metric(f, (g, t, sh, ff, hf, se, z))
    if cur_ch is not None:
        f.write("}")


def _write_timewise(f, conn, movie, fmt, lang, mode) -> None:
    present = [r[0] for r in conn.execute(
        "SELECT DISTINCT entity FROM metrics WHERE movie=? AND format=? "
        "AND language=? AND mode=? AND dim='tm'",
        (movie, fmt, lang, mode),
    )]
    ordered = [s for s in "MAEN" if s in present]
    if not ordered:
        return
    sfirst = True
    for slot in ordered:
        if not sfirst:
            f.write(",")
        sfirst = False
        f.write(jdump(slot))
        f.write(":{")
        cur = conn.execute(
            "SELECT date, gross, tickets, shows, ff, hf, seats, zero_gross "
            "FROM metrics WHERE movie=? AND format=? AND language=? "
            "AND mode=? AND dim='tm' AND entity=? ORDER BY date",
            (movie, fmt, lang, mode, slot),
        )
        dfirst = True
        for date, g, t, sh, ff, hf, se, z in cur:
            if not dfirst:
                f.write(",")
            dfirst = False
            f.write(jdump(date))
            f.write(":")
            f.write(jdump(compact_timewise(g, t, sh, ff, hf, se)))
        f.write("}")


def _write_mode_block(f, conn, movie, fmt, lang, mode) -> None:
    first = True
    for dim, key, writer in (
        ("d", "daily", _write_daily_map),
        ("c", "citywise", _write_citywise),
        ("ch", "chainwise", _write_chainwise),
        ("tm", "timewise", _write_timewise),
    ):
        if not _has_rows(conn, movie, fmt, lang, mode, dim):
            continue
        if not first:
            f.write(",")
        first = False
        f.write(jdump(key))
        f.write(":{")
        writer(f, conn, movie, fmt, lang, mode)
        f.write("}")


def _totals_for_mode(conn, movie, fmt, lang, mode):
    row = conn.execute(
        "SELECT COALESCE(SUM(gross),0), COALESCE(SUM(tickets),0), "
        "COALESCE(SUM(shows),0), COALESCE(SUM(ff),0), COALESCE(SUM(hf),0), "
        "COALESCE(SUM(seats),0), COALESCE(SUM(zero_gross),0) FROM metrics "
        "WHERE movie=? AND format=? AND language=? AND mode=? AND dim='d'",
        (movie, fmt, lang, mode),
    ).fetchone()
    return row if row and row[2] else None


def _write_version(f, conn, movie, fmt, lang, summary_acc) -> None:
    f.write("{")
    parts = []
    if fmt:
        parts.append(('"format":', jdump(fmt)))
    if lang:
        parts.append(('"language":', jdump(lang)))
    for i, (k, v) in enumerate(parts):
        if i:
            f.write(",")
        f.write(k)
        f.write(v)

    for mode, root_name in (("daily", "boxoffice"), ("advance", "advance")):
        exists = conn.execute(
            "SELECT 1 FROM metrics WHERE movie=? AND format=? AND language=? "
            "AND mode=? LIMIT 1",
            (movie, fmt, lang, mode),
        ).fetchone()
        if not exists:
            continue
        f.write(",")
        f.write(jdump(root_name))
        f.write(":{")
        _write_mode_block(f, conn, movie, fmt, lang, mode)
        f.write("}")

    totals_raw: dict[str, tuple] = {}
    for mode, root_name in (("daily", "boxoffice"), ("advance", "advance")):
        obj = _totals_for_mode(conn, movie, fmt, lang, mode)
        if obj:
            totals_raw[root_name] = obj

    if totals_raw:
        f.write(',"totals":{')
        first = True
        for k, v in totals_raw.items():
            if not first:
                f.write(",")
            first = False
            f.write(jdump(k))
            f.write(":")
            f.write(jdump(compact_metric(v)))
        f.write("}")

        box = totals_raw.get("boxoffice")
        if box:
            g, t, sh, ff, hf, se, z = box
            for bucket_key, target in ((fmt, "f"), (lang, "l")):
                if not bucket_key:
                    continue
                acc = summary_acc[target]
                if bucket_key not in acc:
                    acc[bucket_key] = metric_zero()
                add_metric(acc[bucket_key], g, t, sh, ff, hf, se, z)

    f.write("}")


def build_movie(conn, movie: str) -> Path | None:
    """Atomic write: fill <slug>.json.tmp, then os.replace to <slug>.json."""
    variants = conn.execute(
        "SELECT format, language FROM variants WHERE movie=? "
        "ORDER BY format, language", (movie,),
    ).fetchall()
    if not variants:
        return None

    row = conn.execute(
        "SELECT MIN(date), MAX(date) FROM metrics "
        "WHERE movie=? AND mode='daily' AND dim='d'", (movie,),
    ).fetchone()
    if not row or not row[0]:
        return None
    startdate, lastdate = row

    formats = sorted({f for f, _ in variants if f})
    languages = sorted({l for _, l in variants if l})
    slug = slugify(movie)
    if not slug:
        return None

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / f"{slug}.json"
    tmp = OUTPUT_DIR / f"{slug}.json.tmp"
    summary_acc = {"f": {}, "l": {}}

    try:
        with tmp.open("w", encoding="utf-8", newline="\n",
                      buffering=FILE_WRITE_BUFFER) as f:
            f.write("{")
            f.write('"movie":');     f.write(jdump(movie))
            f.write(',"slug":');     f.write(jdump(slug))
            f.write(',"formats":');  f.write(jdump(formats))
            f.write(',"languages":');f.write(jdump(languages))
            f.write(',"startdate":');f.write(jdump(startdate))
            f.write(',"lastdate":'); f.write(jdump(lastdate))
            f.write(',"versions":[')
            first = True
            for fmt, lang in variants:
                if not first:
                    f.write(",")
                first = False
                _write_version(f, conn, movie, fmt, lang, summary_acc)
            f.write("]")
            if summary_acc["f"] or summary_acc["l"]:
                f.write(',"summary":{')
                f.write('"formatwise":{')
                first = True
                for k, v in summary_acc["f"].items():
                    if not first:
                        f.write(",")
                    first = False
                    f.write(jdump(k))
                    f.write(":")
                    f.write(jdump(compact_metric(v)))
                f.write("}")
                f.write(',"languagewise":{')
                first = True
                for k, v in summary_acc["l"].items():
                    if not first:
                        f.write(",")
                    first = False
                    f.write(jdump(k))
                    f.write(":")
                    f.write(jdump(compact_metric(v)))
                f.write("}")
                f.write("}")
            f.write("}")
        os.replace(tmp, out)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink(missing_ok=True)
        raise
    return out


# ============================================================
# JOB PLANNING  (full vs. incremental)
# ============================================================

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
        for i in range(INCREMENTAL_PAST_DAYS, -1, -1):
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
    p.add_argument("--json-every", type=int,
                   default=JSON_REBUILD_EVERY_N_BATCHES)
    p.add_argument("--queue-depth", type=int, default=DB_QUEUE_DEPTH)
    p.add_argument("--low-memory", action="store_true",
                   help="Ultra-low-memory preset (Replit free tier).")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


# ============================================================
# MAIN
# ============================================================

async def amain(args: argparse.Namespace) -> None:
    global MAX_CONCURRENCY, PARSE_THREADS, DB_QUEUE_DEPTH
    global FLUSH_EVERY_N_FILES, FLUSH_MAX_ROWS, JSON_REBUILD_EVERY_N_BATCHES

    if args.low_memory:
        MAX_CONCURRENCY = 6
        PARSE_THREADS = 1
        DB_QUEUE_DEPTH = 3
        FLUSH_EVERY_N_FILES = 10
        FLUSH_MAX_ROWS = 10_000
        JSON_REBUILD_EVERY_N_BATCHES = 20
    else:
        MAX_CONCURRENCY = args.concurrency
        PARSE_THREADS = args.parse_workers
        DB_QUEUE_DEPTH = args.queue_depth
        FLUSH_EVERY_N_FILES = args.flush_files
        FLUSH_MAX_ROWS = args.flush_rows
        JSON_REBUILD_EVERY_N_BATCHES = args.json_every

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
    built_movies_this_run: set[str] = set()

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
                    ttl_dns_cache=300,
                    keepalive_timeout=60,
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
                }
                async with aiohttp.ClientSession(
                    connector=connector, timeout=timeout, headers=headers,
                ) as session:
                    try:
                        stats = await run_fetch_phase(
                            session, conn, db_lock, pending, state, today,
                        )
                        built_movies_this_run = stats.built_movies
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
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
                conn.execute("PRAGMA journal_mode=DELETE").fetchone()
                conn.execute("PRAGMA synchronous=OFF").fetchone()

            t = time.perf_counter()
            log.info("Running ANALYZE…")
            try:
                conn.execute("ANALYZE")
            except sqlite3.OperationalError as e:
                log.warning("ANALYZE failed (non-fatal): %s", e)
            log.info("ANALYZE finished in %.1fs", time.perf_counter() - t)

        # ---- FINAL BUILD PHASE -------------------------------------
        # In incremental mode, `built_movies_this_run` is small.
        # In --rebuild mode it's empty, so we rebuild everything.
        # Otherwise it's usually empty because the fetch phase already
        # rebuilt everything it touched.
        log.info("")
        log.info("================================================")
        log.info(" FINAL BUILD PHASE")
        log.info("================================================")

        with db_lock:
            all_movies = [
                r[0] for r in conn.execute(
                    "SELECT movie FROM variants GROUP BY movie ORDER BY movie"
                ).fetchall()
            ]

        if args.rebuild:
            to_build = all_movies
        else:
            to_build = [m for m in all_movies
                        if m not in built_movies_this_run]

        log.info(
            "Movies total=%d  already built this run=%d  remaining=%d",
            len(all_movies), len(built_movies_this_run), len(to_build),
        )

        total = len(to_build)
        built = 0
        tb = time.perf_counter()

        for i, movie in enumerate(to_build, 1):
            t_movie = time.perf_counter()
            try:
                with db_lock:
                    if build_movie(conn, movie):
                        built += 1
            except Exception as e:
                log.warning("[movie error] %s: %s: %s",
                            movie, type(e).__name__, e)

            dtm = time.perf_counter() - t_movie
            if dtm > 5.0:
                log.info("  [slow] %s: %.2fs", movie, dtm)

            if i % 200 == 0 or i == total:
                el = time.perf_counter() - tb
                rate = i / el if el > 0 else 0.0
                eta = (total - i) / rate if rate > 0 else 0.0
                log.info("Final build: %d/%d  (%.1f/s, ETA %.0fs, rss=%.0fMB)",
                         i, total, rate, eta, rss_mb())
                if i % 500 == 0:
                    gc.collect()
                    with db_lock:
                        with contextlib.suppress(sqlite3.OperationalError):
                            conn.execute("PRAGMA shrink_memory")
                await asyncio.sleep(0)

        state.save()
        with db_lock:
            conn.commit()

        elapsed = time.perf_counter() - started
        log.info("")
        log.info("================================================")
        log.info(" DONE")
        log.info("================================================")
        log.info("Run mode                  : %s", mode)
        log.info("Movies built during fetch : %d", len(built_movies_this_run))
        log.info("Movies built in final     : %d", built)
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
