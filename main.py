#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BFILMY Movie JSON Builder — resumable edition
=============================================

Creates:
    data/<movie-slug>.json

Resumable state:
    data/state.json          per-day fetch record
    data/bfilmy.sqlite3      persistent aggregate DB
    data/bfilmy_builder.log  append-only run log

CLI
---
    python3 bfilmy_builder.py
        Incremental: skip days already fetched (except current/future months).

    python3 bfilmy_builder.py --fresh
        Wipe state, DB and existing movie JSONs, then run from scratch.

    python3 bfilmy_builder.py --rebuild
        Skip fetching. Just rebuild movie JSONs from the existing DB.

    python3 bfilmy_builder.py --refetch-month 2026-05
        Force re-fetch of every planned day in that month.

SOURCE MAP
----------

2023-12-31 and earlier:
    NO data assumed before 2023.

2023-01-01 -> 2025-12-31 DAILY:
    https://bfilmyapi2025.pages.dev/
    daily/data/YYYY/MM-DD_finalsummary.json

2026-01-01 -> 2026-01-31 DAILY:
    https://bfilmyapi2026.pages.dev/
    daily/data/2026/MM-DD_finalsummary.json

2026-01-01 -> 2026-01-31 ADVANCE:
    https://bfilmyapi2026.pages.dev/
    advance/data/2026/MM-DD_finaldetailed.json

2026-02-01 -> yesterday DAILY:
    https://bfilmyapi2026.pages.dev/
    daily/data/2026/MM-DD_finaldetailed.json

2026-02-01 -> yesterday ADVANCE:
    https://bfilmyapi2026.pages.dev/
    advance/data/2026/MM-DD_finaldetailed.json

TODAY / YESTERDAY / FUTURE DAILY:
    https://bfilmyapi.pages.dev/
    daily/data/YYYYMMDD/finaldetailed.json

TODAY / YESTERDAY / FUTURE ADVANCE:
    https://bfilmyapi.pages.dev/
    advance/data/YYYYMMDD/finaldetailed.json

OUTPUT LOGICAL STRUCTURE
------------------------

{
  "movie": "Haiwaan",
  "slug": "haiwaan",
  "formats": ["2D"],
  "languages": ["Hindi"],
  "startdate": "2026-09-21",
  "lastdate": "2026-10-15",

  "versions": [
    {
      "format": "2D",
      "language": "Hindi",

      "boxoffice": {
        "daily":     {...},
        "citywise":  {"Mumbai": {"s": "Maharashtra", "d": {...}}},
        "chainwise": {...},
        "timewise":  {...}
      },

      "advance": {...},

      "stats": {...},

      "totals": {
        "boxoffice": {...},
        "advance":   {...}
      }
    }
  ],

  "summary": {
    "formatwise":   {...},
    "languagewise": {...}
  }
}

COMPACT JSON SCHEMA
-------------------
#
# Metrics use a fixed-position array so repeated keys such as ff/hf/t/se
# are not stored for every date:
#   [g, sh, ff, hf, t, se, z]
#
# g  = gross
# sh = shows
# ff = fast-filling
# hf = housefull
# t  = tickets sold
# se = total seats
# z  = zero-gross shows
#
# Derived values deliberately omitted:
#   occupancy = t / se * 100
#   atp       = g / t
#
# Duplicate aggregates deliberately omitted:
#   daily totals   -> derive by summing citywise dates
#   state totals   -> derive by grouping cities using city "s"
#   overall totals -> derive from city/day data
#   stats/top-*    -> derive from the retained dimensions
#
TIME SLOTS
----------
M = Morning
A = Afternoon
E = Evening
N = Night
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import gc
import json
import logging
import os
import re
import resource
import shutil
import signal
import sqlite3
import sys
import threading
import time
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

import requests

try:
    import ijson  # type: ignore
except ImportError:
    ijson = None


# ============================================================
# CONFIG
# ============================================================

MAX_WORKERS = 30
START_DATE = dt.date(2023, 1, 1)
ADVANCE_FUTURE_DAYS = 45

REQUEST_TIMEOUT = 60
RETRIES = 3
DOWNLOAD_CHUNK_SIZE = 1 << 20

DB_FLUSH_ROWS = 5000
FILE_WRITE_BUFFER = 1 << 20

OUTPUT_DIR = Path("data")
STATE_PATH = OUTPUT_DIR / "state.json"
DB_PATH = OUTPUT_DIR / "bfilmy.sqlite3"
LOG_PATH = OUTPUT_DIR / "bfilmy_builder.log"

TMP_DIR = Path("data") / "_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

STATE_VERSION = 1


# ============================================================
# LOGGING
# ============================================================

log = logging.getLogger("bfilmy")


def setup_logging(verbose: bool = False) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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


# ============================================================
# DATE / TIME HELPERS
# ============================================================

def today_ist() -> dt.date:
    return dt.datetime.now(IST).date()


def date_string(d: dt.date) -> str:
    return d.isoformat()


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
    """Per-day fetch record persisted as JSON."""

    def __init__(self, path: Path):
        self.path = path
        self.created = now_iso()
        self.last_run = self.created
        # date-iso -> {"d": bool, "a": bool}
        self.days: dict[str, dict[str, bool]] = {}

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("version") != STATE_VERSION:
                log.warning(
                    "State file version mismatch (%s != %s); starting fresh",
                    data.get("version"), STATE_VERSION,
                )
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
        payload = {
            "version": STATE_VERSION,
            "created": self.created,
            "last_run": self.last_run,
            "days": self.days,
        }
        tmp = self.path.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=1, sort_keys=True)
        os.replace(tmp, self.path)

    @staticmethod
    def _letter(mode: str) -> str:
        return "d" if mode == "daily" else "a"

    def has(self, date: dt.date, mode: str) -> bool:
        rec = self.days.get(date.isoformat())
        if not rec:
            return False
        return bool(rec.get(self._letter(mode), False))

    def mark(self, date: dt.date, mode: str) -> None:
        key = date.isoformat()
        rec = self.days.get(key)
        if rec is None:
            rec = {}
            self.days[key] = rec
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
# SOURCE ROUTER
# ============================================================

def source_url(d: dt.date, mode: str) -> str | None:
    today = today_ist()

    if d.year <= 2025:
        if mode != "daily":
            return None
        return (
            "https://bfilmyapi2025.pages.dev/"
            f"daily/data/{d.year}/"
            f"{d:%m-%d}_finalsummary.json"
        )

    if d.year == 2026 and d.month == 1:
        if mode == "daily":
            return (
                "https://bfilmyapi2026.pages.dev/"
                "daily/data/2026/"
                f"{d:%m-%d}_finalsummary.json"
            )
        if mode == "advance":
            return (
                "https://bfilmyapi2026.pages.dev/"
                "advance/data/2026/"
                f"{d:%m-%d}_finaldetailed.json"
            )

    if d >= today - dt.timedelta(days=1):
        return (
            "https://bfilmyapi.pages.dev/"
            f"{mode}/data/{d:%Y%m%d}/"
            "finaldetailed.json"
        )

    return (
        "https://bfilmyapi2026.pages.dev/"
        f"{mode}/data/2026/"
        f"{d:%m-%d}_finaldetailed.json"
    )


# ============================================================
# HTTP
# ============================================================

_thread_local = threading.local()


def get_session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (BFILMY Movie JSON Builder)",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Encoding": "gzip, deflate",
        })
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=2,
            pool_maxsize=4,
            max_retries=0,
            pool_block=False,
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _thread_local.session = s
    return s


def download_source(
    date: dt.date,
    mode: str,
    url: str,
) -> tuple[dt.date, str, str | None]:

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    filename = (
        f"{date.isoformat()}_{mode}_"
        f"{threading.get_ident()}_{time.time_ns()}.json"
    )
    path = TMP_DIR / filename
    session = get_session()

    for attempt in range(1, RETRIES + 1):
        try:
            with session.get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
                if r.status_code == 404:
                    return date, mode, None
                r.raise_for_status()
                with path.open("wb", buffering=1 << 20) as f:
                    for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
            return date, mode, str(path)
        except Exception:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass
            if attempt < RETRIES:
                time.sleep(0.5 * attempt)

    return date, mode, None


# ============================================================
# NUMBER HELPERS
# ============================================================

def to_float(v: Any) -> float:
    try:
        if v in (None, ""):
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def to_int(v: Any) -> int:
    try:
        if v in (None, ""):
            return 0
        return int(float(v))
    except Exception:
        return 0


def occupancy(tickets, seats) -> float:
    if seats <= 0:
        return 0.0
    return round(tickets / seats * 100, 2)


def atp(gross, tickets) -> float:
    if tickets <= 0:
        return 0.0
    return round(gross / tickets, 2)


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


BRACKET_RE = re.compile(r"\[([^\]]+)\]\s*$")


def parse_movie_variant(raw: str) -> tuple[str, str, str]:
    raw = str(raw).strip()
    m = BRACKET_RE.search(raw)
    if not m:
        return raw, "", ""
    movie = raw[: m.start()].strip()
    inside = m.group(1).strip()
    parts = [x.strip() for x in inside.split("|", 1)]
    if len(parts) == 2:
        return movie, parts[0], parts[1]
    return movie, parts[0], ""


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
    if (
        seats > 0
        and available is not None
        and to_int(available) == 0
        and sold >= seats
    ):
        return 1
    return 0


# ============================================================
# DB
# ============================================================

def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=180, isolation_level=None)

    conn.execute("PRAGMA journal_mode=WAL").fetchone()
    conn.execute("PRAGMA synchronous=NORMAL").fetchone()
    conn.execute("PRAGMA temp_store=MEMORY").fetchone()
    conn.execute("PRAGMA cache_size=-131072").fetchone()   # 128 MB
    conn.execute("PRAGMA wal_autocheckpoint=1000").fetchone()
    conn.execute("PRAGMA mmap_size=1073741824").fetchone()  # 1 GiB
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
    mode, dim, date,
    movie, format, language,
    entity, state,
    gross, tickets, shows, ff, hf, seats, zero_gross
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (mode, dim, date, movie, format, language, entity)
DO UPDATE SET
    gross      = excluded.gross,
    tickets    = excluded.tickets,
    shows      = excluded.shows,
    ff         = excluded.ff,
    hf         = excluded.hf,
    seats      = excluded.seats,
    zero_gross = excluded.zero_gross,
    state      = CASE
                    WHEN excluded.state <> '' THEN excluded.state
                    ELSE state
                 END
"""


def ensure_variant(conn, movie: str, fmt: str, lang: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO variants (movie, format, language) VALUES (?, ?, ?)",
        (movie, fmt, lang),
    )


def _flush_rows(conn: sqlite3.Connection, rows: list) -> None:
    if rows:
        conn.executemany(UPSERT_SQL, rows)
        rows.clear()


# ============================================================
# METRIC BUFFER
# ============================================================

def metric_zero() -> list:
    return [0.0, 0, 0, 0, 0, 0, 0]


def add_metric(target, gross, tickets, shows, ff, hf, seats, zero) -> None:
    target[0] += gross
    target[1] += tickets
    target[2] += shows
    target[3] += ff
    target[4] += hf
    target[5] += seats
    target[6] += zero


def metric_object(v) -> dict:
    g, t, sh, ff, hf, se, z = v
    return {
        "g": round(g, 2),
        "o": occupancy(t, se),
        "sh": int(sh),
        "ff": int(ff),
        "hf": int(hf),
        "t": int(t),
        "se": int(se),
        "z": int(z),
    }


# ============================================================
# STREAMING READERS
# ============================================================

def stream_detailed_rows(path: str) -> Iterator[dict]:
    if ijson is not None:
        with open(path, "rb") as f:
            yield from ijson.items(f, "data.item")
        return
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    for row in payload.get("data", []):
        yield row


def stream_summary_movies(path: str) -> Iterator[tuple[str, dict]]:
    if ijson is not None:
        with open(path, "rb") as f:
            yield from ijson.kvitems(f, "movies")
        return
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    for movie, data in payload.get("movies", {}).items():
        yield movie, data


# ============================================================
# PROCESS DETAILED FILE
# ============================================================

def process_detailed_file(path, date, mode, conn) -> set[str]:
    date_str = date_string(date)
    include_timewise = date >= dt.date(2026, 2, 1)
    buckets: dict[tuple[str, str, str], dict] = {}
    touched_movies: set[str] = set()

    conn.execute("BEGIN IMMEDIATE")
    try:
        # Idempotency: wipe today's rows for this mode before inserting.
        conn.execute(
            "DELETE FROM metrics WHERE mode = ? AND date = ?",
            (mode, date_str),
        )

        for row in stream_detailed_rows(path):
            if not isinstance(row, dict):
                continue
            raw_movie = row.get("movie")
            if not raw_movie:
                continue

            movie, fmt, lang = parse_movie_variant(raw_movie)
            touched_movies.add(movie)
            key = (movie, fmt, lang)

            bucket = buckets.get(key)
            if bucket is None:
                bucket = {
                    "daily": metric_zero(),
                    "cities": {},
                    "chains": {},
                    "times": {
                        "M": metric_zero(), "A": metric_zero(),
                        "E": metric_zero(), "N": metric_zero(),
                    },
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
                    add_metric(bucket["times"][slot],
                               gross, tickets, 1, ff, hf, seats, zero)

        db_rows: list = []
        for (movie, fmt, lang), bucket in buckets.items():
            ensure_variant(conn, movie, fmt, lang)

            db_rows.append((
                mode, "d", date_str, movie, fmt, lang, "", "",
                *bucket["daily"],
            ))

            for city, cd in bucket["cities"].items():
                db_rows.append((
                    mode, "c", date_str, movie, fmt, lang, city, cd["state"],
                    *cd["metric"],
                ))
                if len(db_rows) >= DB_FLUSH_ROWS:
                    _flush_rows(conn, db_rows)

            for chain, vals in bucket["chains"].items():
                db_rows.append((
                    mode, "ch", date_str, movie, fmt, lang, chain, "",
                    *vals,
                ))
                if len(db_rows) >= DB_FLUSH_ROWS:
                    _flush_rows(conn, db_rows)

            if include_timewise:
                for slot in ("M", "A", "E", "N"):
                    vals = bucket["times"][slot]
                    if vals[2] == 0:
                        continue
                    db_rows.append((
                        mode, "tm", date_str, movie, fmt, lang, slot, "",
                        *vals,
                    ))

        _flush_rows(conn, db_rows)
        conn.execute("COMMIT")

    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise
    finally:
        buckets.clear()

    return touched_movies


# ============================================================
# PROCESS SUMMARY FILE
# ============================================================

def process_summary_file(path, date, conn) -> set[str]:
    date_str = date_string(date)
    touched_movies: set[str] = set()

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "DELETE FROM metrics WHERE mode = ? AND date = ?",
            ("daily", date_str),
        )

        db_rows: list = []

        for raw_movie, data in stream_summary_movies(path):
            if not isinstance(data, dict):
                continue

            movie, fmt, lang = parse_movie_variant(raw_movie)
            touched_movies.add(movie)
            ensure_variant(conn, movie, fmt, lang)

            gross = to_float(data.get("gross"))
            tickets = to_int(data.get("sold"))
            shows = to_int(data.get("shows"))
            seats = to_int(data.get("totalSeats"))
            ff = to_int(data.get("fastfilling"))
            hf = to_int(data.get("housefull"))

            db_rows.append((
                "daily", "d", date_str, movie, fmt, lang, "", "",
                gross, tickets, shows, ff, hf, seats, 0,
            ))

            for item in (data.get("details") or []):
                if not isinstance(item, dict):
                    continue
                city = str(item.get("city") or "").strip()
                if not city:
                    continue
                db_rows.append((
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
                if len(db_rows) >= DB_FLUSH_ROWS:
                    _flush_rows(conn, db_rows)

            for item in (data.get("Chain_details") or []):
                if not isinstance(item, dict):
                    continue
                chain = str(item.get("chain") or "").strip()
                if not chain:
                    continue
                db_rows.append((
                    "daily", "ch", date_str, movie, fmt, lang, chain, "",
                    to_float(item.get("gross")),
                    to_int(item.get("sold")),
                    to_int(item.get("shows")),
                    to_int(item.get("fastfilling")),
                    to_int(item.get("housefull")),
                    to_int(item.get("totalSeats")),
                    0,
                ))
                if len(db_rows) >= DB_FLUSH_ROWS:
                    _flush_rows(conn, db_rows)

        _flush_rows(conn, db_rows)
        conn.execute("COMMIT")

    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise

    return touched_movies


def process_downloaded(date, mode, path, conn) -> set[str]:
    try:
        if mode == "daily" and date <= dt.date(2026, 1, 31):
            touched = process_summary_file(path, date, conn)
        else:
            touched = process_detailed_file(path, date, mode, conn)
    finally:
        try:
            os.remove(path)
        except OSError:
            pass
    return touched


# ============================================================
# STATS (pure SQL)
# ============================================================

def calculate_stats_sql(conn, movie, fmt, lang) -> dict:
    stats: dict = {}
    params = (movie, fmt, lang, "daily")

    row = conn.execute(
        """
        SELECT entity, SUM(gross) AS g
        FROM metrics
        WHERE movie=? AND format=? AND language=? AND mode=? AND dim='c'
        GROUP BY entity ORDER BY g DESC LIMIT 1
        """,
        params,
    ).fetchone()
    if row and row[0]:
        stats["top_city"] = {"name": row[0], "g": round(row[1] or 0.0, 2)}

    row = conn.execute(
        """
        SELECT state, SUM(gross) AS g
        FROM metrics
        WHERE movie=? AND format=? AND language=? AND mode=? AND dim='c'
          AND state <> ''
        GROUP BY state ORDER BY g DESC LIMIT 1
        """,
        params,
    ).fetchone()
    if row and row[0]:
        stats["top_state"] = {"name": row[0], "g": round(row[1] or 0.0, 2)}

    row = conn.execute(
        """
        SELECT entity, SUM(gross) AS g
        FROM metrics
        WHERE movie=? AND format=? AND language=? AND mode=? AND dim='ch'
        GROUP BY entity ORDER BY g DESC LIMIT 1
        """,
        params,
    ).fetchone()
    if row and row[0]:
        stats["top_chain"] = {"name": row[0], "g": round(row[1] or 0.0, 2)}

    best_slot, best_occ = None, -1.0
    for slot, t, se in conn.execute(
        """
        SELECT entity, SUM(tickets), SUM(seats)
        FROM metrics
        WHERE movie=? AND format=? AND language=? AND mode=? AND dim='tm'
        GROUP BY entity
        """,
        params,
    ):
        if se and se > 0:
            occ = t / se * 100.0
            if occ > best_occ:
                best_occ = occ
                best_slot = slot
    if best_slot:
        stats["best_occupancy_time"] = best_slot

    return stats


# ============================================================
# JSON WRITER
# ============================================================

_J_KW = {"ensure_ascii": False, "separators": (",", ":")}


def jdump(obj: Any) -> str:
    return json.dumps(obj, **_J_KW)


def metric_array(v) -> list:
    """Compact fixed-order metric array: [g, sh, ff, hf, t, se, z]."""
    g, t, sh, ff, hf, se, z = v
    return [
        round(g, 2),
        int(sh),
        int(ff),
        int(hf),
        int(t),
        int(se),
        int(z),
    ]


def _has_rows(conn, movie, fmt, lang, mode, dim) -> bool:
    return conn.execute(
        """
        SELECT 1 FROM metrics
        WHERE movie=? AND format=? AND language=? AND mode=? AND dim=?
        LIMIT 1
        """,
        (movie, fmt, lang, mode, dim),
    ).fetchone() is not None


def _write_citywise(f, conn, movie, fmt, lang, mode) -> None:
    """
    Primary geographical data.

    {
      "Mumbai":{
        "s":"Maharashtra",
        "d":{
          "2026-09-01":[g,sh,ff,hf,t,se,z]
        }
      }
    }

    Daywise totals and statewise totals are intentionally derived from this,
    so storing a separate daily map would duplicate the same information.
    """
    cur = conn.execute(
        """
        SELECT entity, state, date,
               gross, tickets, shows, ff, hf, seats, zero_gross
        FROM metrics
        WHERE movie=? AND format=? AND language=? AND mode=? AND dim='c'
        ORDER BY entity, date
        """,
        (movie, fmt, lang, mode),
    )

    current_city = None
    city_first = True
    date_first = True

    for city, state, date, g, t, sh, ff, hf, se, z in cur:
        if city != current_city:
            if current_city is not None:
                f.write("}}")
            if not city_first:
                f.write(",")
            city_first = False

            f.write(jdump(city))
            f.write(":{")
            if state:
                f.write('"s":')
                f.write(jdump(state))
                f.write(",")
            f.write('"d":{')

            current_city = city
            date_first = True

        if not date_first:
            f.write(",")
        date_first = False

        f.write(jdump(date))
        f.write(":")
        f.write(jdump(metric_array((g, t, sh, ff, hf, se, z))))

    if current_city is not None:
        f.write("}}")


def _write_chainwise(f, conn, movie, fmt, lang, mode) -> None:
    """
    Chain totals by date. These cannot be reconstructed from city data,
    so they are retained.
    """
    cur = conn.execute(
        """
        SELECT entity, date,
               gross, tickets, shows, ff, hf, seats, zero_gross
        FROM metrics
        WHERE movie=? AND format=? AND language=? AND mode=? AND dim='ch'
        ORDER BY entity, date
        """,
        (movie, fmt, lang, mode),
    )

    current_chain = None
    chain_first = True
    date_first = True

    for chain, date, g, t, sh, ff, hf, se, z in cur:
        if chain != current_chain:
            if current_chain is not None:
                f.write("}")
            if not chain_first:
                f.write(",")
            chain_first = False

            f.write(jdump(chain))
            f.write(":{")
            current_chain = chain
            date_first = True

        if not date_first:
            f.write(",")
        date_first = False

        f.write(jdump(date))
        f.write(":")
        f.write(jdump(metric_array((g, t, sh, ff, hf, se, z))))

    if current_chain is not None:
        f.write("}")


def _write_timewise(f, conn, movie, fmt, lang, mode) -> None:
    """
    Time-slot totals by date.

    M = Morning
    A = Afternoon
    E = Evening
    N = Night

    Occupancy and ATP are derived from the stored metric array.
    """
    present = [
        r[0] for r in conn.execute(
            """
            SELECT DISTINCT entity FROM metrics
            WHERE movie=? AND format=? AND language=? AND mode=? AND dim='tm'
            """,
            (movie, fmt, lang, mode),
        )
    ]
    ordered = [s for s in ("M", "A", "E", "N") if s in present]
    if not ordered:
        return

    slot_first = True
    for slot in ordered:
        if not slot_first:
            f.write(",")
        slot_first = False

        f.write(jdump(slot))
        f.write(":{")

        cur = conn.execute(
            """
            SELECT date, gross, tickets, shows, ff, hf, seats, zero_gross
            FROM metrics
            WHERE movie=? AND format=? AND language=? AND mode=?
              AND dim='tm' AND entity=?
            ORDER BY date
            """,
            (movie, fmt, lang, mode, slot),
        )

        date_first = True
        for date, g, t, sh, ff, hf, se, z in cur:
            if not date_first:
                f.write(",")
            date_first = False

            f.write(jdump(date))
            f.write(":")
            f.write(jdump(metric_array((g, t, sh, ff, hf, se, z))))

        f.write("}")


def _write_mode_block(f, conn, movie, fmt, lang, mode) -> None:
    """
    Compact mode block.

      b = box office
      a = advance

    Dimensions:
      c  = citywise
      ch = chainwise
      tm = timewise
    """
    first = True

    for dim, key in (("c", "c"), ("ch", "ch"), ("tm", "tm")):
        if not _has_rows(conn, movie, fmt, lang, mode, dim):
            continue

        if not first:
            f.write(",")
        first = False

        f.write(jdump(key))
        f.write(":{")

        if dim == "c":
            _write_citywise(f, conn, movie, fmt, lang, mode)
        elif dim == "ch":
            _write_chainwise(f, conn, movie, fmt, lang, mode)
        else:
            _write_timewise(f, conn, movie, fmt, lang, mode)

        f.write("}")


def _write_version(f, conn, movie, fmt, lang) -> None:
    """
    Writes one movie variant.

    Compact output example:

    {
      "f":"2D",
      "l":"Hindi",
      "b":{
        "c":{
          "Mumbai":{
            "s":"Maharashtra",
            "d":{
              "2026-09-21":[12.5,120,8,2,850,1200,5]
            }
          }
        },
        "ch":{...},
        "tm":{...}
      },
      "a":{...}
    }
    """
    f.write("{")
    first = True

    if fmt:
        f.write('"f":')
        f.write(jdump(fmt))
        first = False

    if lang:
        if not first:
            f.write(",")
        f.write('"l":')
        f.write(jdump(lang))
        first = False

    for mode, root_name in (("daily", "b"), ("advance", "a")):
        exists = conn.execute(
            """
            SELECT 1
            FROM metrics
            WHERE movie=? AND format=? AND language=? AND mode=?
            LIMIT 1
            """,
            (movie, fmt, lang, mode),
        ).fetchone()

        if not exists:
            continue

        if not first:
            f.write(",")
        first = False

        f.write(jdump(root_name))
        f.write(":{")
        _write_mode_block(f, conn, movie, fmt, lang, mode)
        f.write("}")

    f.write("}")


def build_movie(conn, movie: str) -> Path | None:
    variants = conn.execute(
        """
        SELECT format, language
        FROM variants
        WHERE movie=?
        ORDER BY format, language
        """,
        (movie,),
    ).fetchall()

    if not variants:
        return None

    row = conn.execute(
        """
        SELECT MIN(date), MAX(date)
        FROM metrics
        WHERE movie=? AND mode='daily' AND dim='d'
        """,
        (movie,),
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
    out_path = OUTPUT_DIR / f"{slug}.json"

    # Never write directly to the published JSON. A crash/kill halfway through
    # serialization must leave the previous good file untouched.
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")

    try:
        with tmp_path.open(
            "w",
            encoding="utf-8",
            newline="\n",
            buffering=FILE_WRITE_BUFFER,
        ) as f:
            f.write("{")

            f.write('"m":')
            f.write(jdump(movie))

            f.write(',"s":')
            f.write(jdump(slug))

            f.write(',"f":')
            f.write(jdump(formats))

            f.write(',"l":')
            f.write(jdump(languages))

            f.write(',"sd":')
            f.write(jdump(startdate))

            f.write(',"ld":')
            f.write(jdump(lastdate))

            f.write(',"v":[')

            first = True
            for fmt, lang in variants:
                if not first:
                    f.write(",")
                first = False
                _write_version(f, conn, movie, fmt, lang)

            f.write("]}")
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, out_path)

    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise

    return out_path


# ============================================================
# JOB PLANNING
# ============================================================

def generate_jobs() -> list[tuple[dt.date, str, str]]:
    today = today_ist()
    jobs: list[tuple[dt.date, str, str]] = []

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

    return jobs


def should_fetch(
    date: dt.date, mode: str, state: State, today: dt.date, conn: sqlite3.Connection,
) -> bool:
    """Resume strictly from persisted state/database.

    A date already marked complete is skipped on the next run, including
    current/future dates. Use --refresh-current to intentionally re-fetch
    the mutable current month.
    """
    if state.has(date, mode):
        return False
    # If state was lost but DB already contains this date, treat it as done.
    row = conn.execute(
        "SELECT 1 FROM metrics WHERE mode=? AND date=? LIMIT 1",
        (mode, date.isoformat()),
    ).fetchone()
    return row is None


# ============================================================
# MONTH STATUS REPORT
# ============================================================

def report_month_status(
    plan: list[tuple[dt.date, str, str]],
    state: State,
    today: dt.date,
) -> None:
    months: dict[str, dict] = defaultdict(lambda: {
        "daily":   {"planned": 0, "done": 0},
        "advance": {"planned": 0, "done": 0},
    })

    for d, mode, _ in plan:
        k = f"{d.year:04d}-{d.month:02d}"
        months[k][mode]["planned"] += 1
        if state.has(d, mode):
            months[k][mode]["done"] += 1

    current_key = f"{today.year:04d}-{today.month:02d}"

    log.info("")
    log.info("================================================")
    log.info(" MONTH STATUS (state.json)")
    log.info("================================================")

    incomplete: list[tuple[str, str, int, int]] = []

    for month in sorted(months):
        parts = []
        for mode in ("daily", "advance"):
            p = months[month][mode]["planned"]
            dn = months[month][mode]["done"]
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
        log.info(f"  {month}  " + "  ".join(parts) + tag)

    log.info("")
    if incomplete:
        log.info("INCOMPLETE PAST MONTHS:")
        for month, mode, dn, p in incomplete:
            log.info(f"  !! {month}  {mode}  {dn}/{p}")
    else:
        log.info("All past months are complete.")


# ============================================================
# PHASE 1 — FETCH
# ============================================================

_stop = threading.Event()


def _install_sigint() -> None:
    def handler(signum, frame):
        if _stop.is_set():
            log.warning("Second SIGINT: exiting immediately")
            os._exit(130)
        _stop.set()
        log.warning("SIGINT received: will finish current batch then exit")
    try:
        signal.signal(signal.SIGINT, handler)
    except ValueError:
        pass  # not in main thread


def run_fetch_phase(
    conn: sqlite3.Connection,
    state: State,
    jobs: list[tuple[dt.date, str, str]],
) -> None:
    """Fetch, commit, and dump immediately after every successful day.

    This is intentionally checkpointed at job granularity, not batch
    granularity: Ctrl-C or a crash loses at most the currently processing
    source file. The DB + state file + movie JSONs are all persisted before
    moving on to the next completed result.
    """
    total = len(jobs)
    done = ok = missing = failed = dumped = 0
    batch_size = MAX_WORKERS
    batches = (total + batch_size - 1) // batch_size

    for batch_idx, batch_start in enumerate(range(0, total, batch_size), 1):
        if _stop.is_set():
            log.warning("Stopping before batch %d/%d", batch_idx, batches)
            break

        batch = jobs[batch_start: batch_start + batch_size]
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_WORKERS,
            thread_name_prefix="bfilmy",
        ) as ex:
            futs = {ex.submit(download_source, d, m, u): (d, m) for d, m, u in batch}

            for fut in concurrent.futures.as_completed(futs):
                d, m = futs[fut]
                path = None
                try:
                    d, m, path = fut.result()
                except Exception as e:
                    failed += 1
                    done += 1
                    log.warning("[download fail] %s %s: %s", d, m, e)
                    continue

                if not path:
                    missing += 1
                    done += 1
                    # Historical 404s can be checkpointed, but future/current
                    # advance files may appear later and must be retried.
                    if d < today_ist():
                        state.mark(d, m)
                        state.save()
                    log.info("[missing] %s %s", d, m)
                    continue

                try:
                    touched = process_downloaded(d, m, path, conn)
                    conn.commit()

                    # Persist the checkpoint BEFORE dumping so a crash during JSON
                    # generation never causes the source day to be downloaded again.
                    state.mark(d, m)
                    state.save()

                    # Live dump: only movies affected by this source day.
                    for movie in sorted(touched):
                        try:
                            out = build_movie(conn, movie)
                            if out:
                                dumped += 1
                        except Exception as e:
                            # Source data remains safely committed. The movie can be
                            # rebuilt on the next run with --rebuild.
                            log.warning(
                                "[dump fail] %s after %s %s: %s: %s",
                                movie, d, m, type(e).__name__, e,
                            )
                    ok += 1
                    log.info("[saved] %s %s  movies=%d", d, m, len(touched))
                except Exception as e:
                    conn.rollback()
                    failed += 1
                    log.warning(
                        "[proc fail] %s %s: %s: %s",
                        d, m, type(e).__name__, e,
                    )
                finally:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

                done += 1
                if _stop.is_set():
                    break

        # Extra checkpoint after each worker batch.
        state.save()
        log.info(
            "  batch %d/%d  ok=%d missing=%d failed=%d dumped=%d (%d/%d)",
            batch_idx, batches, ok, missing, failed, dumped, done, total,
        )

        if _stop.is_set():
            log.warning("Pause requested; saved all completed jobs in this batch.")
            break

    log.info(
        "Fetch/live-dump complete: ok=%d missing=%d failed=%d movie-dumps=%d",
        ok, missing, failed, dumped,
    )


# ============================================================
# PHASE 2 — BUILD MOVIE JSONS
# ============================================================

def run_build_phase(conn: sqlite3.Connection) -> int:
    log.info("")
    log.info("================================================")
    log.info(" BUILDING MOVIE FILES")
    log.info("================================================")

    rows = conn.execute(
        "SELECT movie FROM variants GROUP BY movie ORDER BY movie"
    ).fetchall()
    total = len(rows)
    built = 0
    t0 = time.perf_counter()

    for i, (movie,) in enumerate(rows, 1):
        t_movie = time.perf_counter()
        try:
            if build_movie(conn, movie):
                built += 1
        except Exception as e:
            log.warning("[movie error] %s: %s: %s", movie, type(e).__name__, e)

        dt_movie = time.perf_counter() - t_movie
        if dt_movie > 5.0:
            log.info("  [slow] %s: %.2fs", movie, dt_movie)

        if i % 50 == 0 or i == total:
            elapsed = time.perf_counter() - t0
            rate = i / elapsed if elapsed > 0 else 0.0
            eta = (total - i) / rate if rate > 0 else 0.0
            log.info(
                "Movies: %d/%d  (%.1f/s, ETA %.0fs)",
                i, total, rate, eta,
            )
            if i % 500 == 0:
                gc.collect()
                try:
                    conn.execute("PRAGMA shrink_memory")
                except sqlite3.OperationalError:
                    pass

    return built


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="BFILMY movie JSON builder (resumable)",
    )
    p.add_argument(
        "--fresh", action="store_true",
        help="Wipe state, DB and existing movie JSONs, then run from scratch.",
    )
    p.add_argument(
        "--rebuild", action="store_true",
        help="Skip fetching; only rebuild movie JSONs from the existing DB.",
    )
    p.add_argument(
        "--refetch-month", metavar="YYYY-MM", default=None,
        help="Force re-fetch of a specific month (e.g. 2026-05).",
    )
    p.add_argument(
        "--refresh-current", action="store_true",
        help="Re-fetch checkpointed current-month/future dates (useful for live updates).",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    started = time.perf_counter()
    args = parse_args()

    setup_logging(verbose=args.verbose)
    _install_sigint()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    today = today_ist()

    # ---- startup banner ----------------------------------------------
    log.info("")
    log.info("================================================")
    log.info(" BFILMY MOVIE JSON GENERATOR")
    log.info("================================================")
    log.info("Today IST       : %s", today)
    log.info("Workers         : %d", MAX_WORKERS)
    log.info("Advance horizon : %d days", ADVANCE_FUTURE_DAYS)
    log.info("ijson available : %s", ijson is not None)
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        log.info("fd limit        : soft=%s hard=%s", soft, hard)
        need = MAX_WORKERS * 4 + 128
        if soft < need:
            log.warning(
                "  WARNING: soft fd limit %s < recommended %s. "
                "Run: ulimit -n %s",
                soft, need, need,
            )
    except Exception:
        pass
    if ijson is None:
        log.warning("  WARNING: ijson not installed. Install with: pip install ijson")

    # ---- --fresh -----------------------------------------------------
    if args.fresh:
        log.warning("--fresh: wiping state, DB and existing movie JSONs")
        try:
            if STATE_PATH.exists():
                STATE_PATH.unlink()
            for suffix in ("", "-wal", "-shm"):
                f = DB_PATH.with_name(DB_PATH.name + suffix)
                if f.exists():
                    f.unlink()
            for f in OUTPUT_DIR.glob("*.json"):
                if f.name != "state.json":
                    f.unlink()
        except Exception as e:
            log.warning("  --fresh wipe error: %s", e)

    # ---- state -------------------------------------------------------
    state = State(STATE_PATH)
    state.load()
    log.info("State file      : %s", STATE_PATH)
    log.info("Days in state   : %d", len(state.days))

    # ---- --refetch-month ---------------------------------------------
    if args.refetch_month:
        try:
            y, m = args.refetch_month.split("-")
            year, month = int(y), int(m)
        except Exception:
            log.error("Invalid --refetch-month format; use YYYY-MM")
            sys.exit(2)

        removed = state.clear_month(year, month)
        log.warning(
            "--refetch-month %04d-%02d: cleared %d state entries",
            year, month, removed,
        )

        # Also delete that month's rows so the fetch is clean.
        conn_tmp = open_database(DB_PATH)
        try:
            cur = conn_tmp.execute(
                "DELETE FROM metrics WHERE date LIKE ?",
                (f"{year:04d}-{month:02d}-%",),
            )
            log.warning(
                "--refetch-month: removed %d metric rows for %04d-%02d",
                cur.rowcount, year, month,
            )
        finally:
            conn_tmp.commit()
            conn_tmp.close()

    # ---- plan --------------------------------------------------------
    plan = generate_jobs()
    log.info("Total planned days: %d", len(plan))

    report_month_status(plan, state, today)

    # ---- DB ----------------------------------------------------------
    conn = open_database(DB_PATH)

    # ---- phase 1 -----------------------------------------------------
    if not args.rebuild:
        pending = [
            (d, m, u) for d, m, u in plan
            if (args.refresh_current and d >= today.replace(day=1))
            or should_fetch(d, m, state, today, conn)
        ]
        log.info("")
        log.info("================================================")
        log.info(" FETCH PHASE")
        log.info("================================================")
        log.info("Pending jobs    : %d of %d", len(pending), len(plan))

        if pending:
            try:
                run_fetch_phase(conn, state, pending)
            finally:
                state.save()
                conn.commit()
        else:
            log.info("Nothing to fetch; all past days are already in state.")
    else:
        log.info("--rebuild: skipping fetch phase")

    # ---- WAL housekeeping -------------------------------------------
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
        conn.execute("PRAGMA journal_mode=DELETE").fetchone()
        conn.execute("PRAGMA synchronous=OFF").fetchone()
    except sqlite3.OperationalError as e:
        log.debug("WAL switch failed (non-fatal): %s", e)

    # ---- ANALYZE (bounded, silent but timed) ------------------------
    t = time.perf_counter()
    log.info("Running ANALYZE…")
    try:
        conn.execute("ANALYZE")
    except sqlite3.OperationalError as e:
        log.warning("ANALYZE failed (non-fatal): %s", e)
    log.info("ANALYZE finished in %.1fs", time.perf_counter() - t)

    # ---- phase 2 -----------------------------------------------------
    built = run_build_phase(conn)

    # ---- finalize ----------------------------------------------------
    state.save()
    conn.commit()
    conn.close()

    elapsed = time.perf_counter() - started
    log.info("")
    log.info("================================================")
    log.info(" DONE")
    log.info("================================================")
    log.info("Movies written : %d", built)
    log.info("Days tracked   : %d", len(state.days))
    log.info("Time           : %.2fs", elapsed)
    log.info("Output dir     : %s", OUTPUT_DIR.resolve())
    log.info("Log file       : %s", LOG_PATH.resolve())
    log.info("")


if __name__ == "__main__":
    main()
