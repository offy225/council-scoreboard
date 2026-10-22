#!/usr/bin/env python3
"""sync_hummingbot.py — Hummingbot paper-trading lane sync for the Council Scoreboard.

Reads Hummingbot's local trade history (SQLite trades DB, or a CSV fallback
exported from `hbot history`) and emits `hummingbot_lane.json` in a stable
schema the terminal/dashboard consumes.

Design rules (house law):
  * Read-only. The SQLite DB is opened with a `mode=ro` URI and is never
    written to. The only file this script writes is the output JSON.
  * Honest numbers only. Anything that cannot be derived from the source is
    emitted as null with an explanatory note — never fabricated, never zero
    dressed up as data. In particular, net PnL is NOT computed from the
    trades DB alone: Hummingbot computes PnL from portfolio values
    (see hummingbot_prep.md), so from SQLite we report net_pnl_usdt=null.
    If a CSV export carries an explicit PnL column, that value is used and
    the note says where it came from.
  * Defensive schema discovery. Hummingbot's SQLite table/column names are
    not part of a documented stable API, so tables and columns are found by
    name patterns and verified by shape before use.
  * Stdlib only. No third-party imports, no credentials in code. The push
    secret (if any) comes from the HB_SYNC_SECRET environment variable.

Usage:
    python3 sync_hummingbot.py                      # one-shot, auto source
    python3 sync_hummingbot.py --source sqlite --db hummingbot/data/conf_paper_bot.sqlite
    python3 sync_hummingbot.py --source csv --csv path/to/history.csv
    python3 sync_hummingbot.py --watch              # re-emit whenever the source changes
    python3 sync_hummingbot.py --watch --push-url https://example.invalid/api/hummingbot-lane
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_DB = Path("hummingbot/data/conf_paper_bot.sqlite")
DEFAULT_OUT = Path("hummingbot_lane.json")
DEFAULT_STALE_MINUTES = 30.0
DEFAULT_WATCH_INTERVAL = 5.0

PNL_NOTE_SQLITE = (
    "Net PnL is not derivable from the trades DB alone: Hummingbot computes "
    "PnL from portfolio values (current vs hold) in `hbot history`, not from "
    "fills. Use the CSV fallback with an explicit PnL column, or wire a "
    "balance snapshot, before showing a PnL number."
)
PNL_NOTE_NO_DATA = "No source data yet, so no PnL. Nothing has been fabricated."


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------

def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_from_epoch(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fnum(value) -> float | None:
    """Best-effort float parse: handles ints, floats, and strings like
    '1,234.56', '$12.30', '-0.5%'. Returns None when unparseable."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip().replace(",", "").replace("$", "")
        if s.endswith("%"):
            s = s[:-1]
        if s in ("", "-", "n/a", "N/A", "None", "null"):
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def parse_timestamp(value) -> float | None:
    """Parse a timestamp cell into epoch seconds.

    Handles: epoch milliseconds (Hummingbot's usual storage, e.g.
    1761148800000), epoch seconds, numeric strings, and ISO-8601 strings.
    Returns None when the value cannot be interpreted."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        if v > 1e11:  # milliseconds
            return v / 1000.0
        if v > 1e8:  # seconds
            return v
        return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        as_num = fnum(s)
        if as_num is not None and as_num > 1e8:
            return parse_timestamp(as_num)
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    return None


def classify_side(value) -> str | None:
    """Map a side/trade_type cell to 'buy' or 'sell'. Handles 'BUY'/'SELL'
    strings and Hummingbot's TradeType ints (BUY=1, SELL=2)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if int(value) == 1:
            return "buy"
        if int(value) == 2:
            return "sell"
        return None
    s = str(value).strip().lower()
    if "buy" in s or s in ("b", "bid", "long"):
        return "buy"
    if "sell" in s or s in ("s", "ask", "short"):
        return "sell"
    return None


def norm(name: str) -> str:
    """Normalize a column/table name for pattern matching."""
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def pick_column(columns: list[str], patterns: list[str]) -> str | None:
    """Return the first real column whose normalized name matches a pattern,
    trying patterns in order. Patterns are matched as substrings of the
    normalized column name."""
    normalized = {c: norm(c) for c in columns}
    for pat in patterns:
        p = norm(pat)
        for col in columns:
            if p == normalized[col]:
                return col
    for pat in patterns:
        p = norm(pat)
        for col in columns:
            if p in normalized[col]:
                return col
    return None


# --------------------------------------------------------------------------
# SQLite source
# --------------------------------------------------------------------------

TRADE_TABLE_PREFERENCE = ("tradefill", "tradefills", "trades", "fills", "trade", "fill")


def _table_score(name: str) -> int | None:
    n = norm(name)
    if "trade" in n and "fill" in n:
        return 0
    if n in ("trades", "trade", "fills"):
        return 1
    if "fill" in n:
        return 2
    if "trade" in n:
        return 3
    return None


def find_trades_table(conn: sqlite3.Connection) -> str | None:
    """Discover the filled-trades table by name pattern, then verify it has
    a price-ish or amount-ish column before trusting it."""
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    candidates = []
    for (name,) in rows:
        score = _table_score(name)
        if score is not None:
            candidates.append((score, name))
    candidates.sort(key=lambda t: (t[0], t[1]))
    for _, name in candidates:
        cols = table_columns(conn, name)
        if pick_column(cols, ["price"]) or pick_column(cols, ["amount", "quantity"]):
            return name
    return None


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    safe = table.replace('"', '""')
    return [r[1] for r in conn.execute(f'PRAGMA table_info("{safe}")').fetchall()]


def fee_from_json_blob(raw, price: float | None, amount: float | None,
                       quote_asset: str | None) -> tuple[float, bool]:
    """Extract a quote-currency fee from Hummingbot's JSON TradeFee blob
    ({"percent": ..., "percent_token": ..., "flat_fees": [{"token": ...,
    "amount": ...}]}). Returns (fee_in_quote, complete). Fees charged in a
    non-quote token that cannot be valued here mark the result incomplete."""
    try:
        blob = json.loads(raw) if isinstance(raw, str) else raw
    except (ValueError, TypeError):
        return 0.0, False
    if not isinstance(blob, dict):
        return 0.0, False
    total = 0.0
    complete = True
    for flat in blob.get("flat_fees") or []:
        if not isinstance(flat, dict):
            continue
        amt = fnum(flat.get("amount"))
        token = str(flat.get("token", "")).upper()
        if amt is None:
            continue
        if quote_asset and token == quote_asset.upper():
            total += amt
        elif token.endswith("USDT") or token.endswith("USD"):
            total += amt
        elif amt:
            complete = False  # fee in another token; cannot value in quote here
    percent = fnum(blob.get("percent"))
    percent_token = str(blob.get("percent_token") or "").upper()
    if percent:
        if price is not None and amount is not None and (
            not percent_token
            or (quote_asset and percent_token == quote_asset.upper())
            or percent_token.endswith("USD")
        ):
            total += percent * price * amount
        else:
            complete = False
    return total, complete


def read_sqlite(db_path: Path) -> dict:
    """Read the trades DB read-only and return aggregate stats.

    Raises FileNotFoundError / sqlite3.Error / LookupError on unusable input;
    the caller converts those into an honest no-data payload."""
    if not db_path.is_file():
        raise FileNotFoundError(f"SQLite DB not found: {db_path}")
    uri = db_path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        with closing(conn):
            table = find_trades_table(conn)
            if table is None:
                raise LookupError("no trades/fills table found in DB")
            cols = table_columns(conn, table)
            side_col = pick_column(cols, ["trade_type", "side", "direction"])
            price_col = pick_column(cols, ["price"])
            amount_col = pick_column(cols, ["amount", "quantity", "base_amount", "size"])
            ts_col = pick_column(cols, ["timestamp", "created_at", "creation_timestamp", "time"])
            symbol_col = pick_column(cols, ["symbol", "trading_pair", "market_name"])
            quote_col = pick_column(cols, ["quote_asset", "quote"])
            fee_quote_col = pick_column(
                cols, ["trade_fee_in_quote", "fee_in_quote", "fee_quote_amount", "fee_amount"]
            )
            fee_blob_col = None
            if "tradefee" in [norm(c) for c in cols]:
                fee_blob_col = next(c for c in cols if norm(c) == "tradefee")

            wanted = [c for c in {side_col, price_col, amount_col, ts_col,
                                  symbol_col, quote_col, fee_quote_col, fee_blob_col} if c]
            safe_table = table.replace('"', '""')
            select_cols = ", ".join('"%s"' % c.replace('"', '""') for c in wanted) or "*"
            rows = conn.execute(f'SELECT {select_cols} FROM "{safe_table}"').fetchall()
    except sqlite3.Error:
        raise

    trades = 0
    buys = 0
    sells = 0
    unknown_side = 0
    volume = 0.0
    volume_seen = False
    fees = 0.0
    fees_seen = False
    fees_complete = True
    last_ts: float | None = None
    quote_asset: str | None = None

    for row in rows:
        trades += 1
        price = fnum(row[price_col]) if price_col else None
        amount = fnum(row[amount_col]) if amount_col else None
        if quote_col and row[quote_col]:
            quote_asset = str(row[quote_col]).upper()
        elif symbol_col and row[symbol_col] and "-" in str(row[symbol_col]):
            quote_asset = str(row[symbol_col]).rsplit("-", 1)[-1].upper()
        side = classify_side(row[side_col]) if side_col else None
        if side == "buy":
            buys += 1
        elif side == "sell":
            sells += 1
        else:
            unknown_side += 1
        if price is not None and amount is not None:
            volume += price * amount
            volume_seen = True
        # Fees: prefer a direct fee-in-quote column; fall back to the JSON blob.
        if fee_quote_col is not None:
            f = fnum(row[fee_quote_col])
            if f is not None:
                fees += f
                fees_seen = True
        elif fee_blob_col is not None and row[fee_blob_col] not in (None, "", 0):
            f, complete = fee_from_json_blob(row[fee_blob_col], price, amount, quote_asset)
            fees += f
            fees_seen = True
            fees_complete = fees_complete and complete
        ts = parse_timestamp(row[ts_col]) if ts_col else None
        if ts is not None and (last_ts is None or ts > last_ts):
            last_ts = ts

    if trades and unknown_side == trades:
        buys = None
        sells = None

    return {
        "trades": trades,
        "buys": buys,
        "sells": sells,
        "unknown_side": unknown_side,
        "volume": volume,
        "volume_seen": volume_seen,
        "fees": fees,
        "fees_seen": fees_seen,
        "fees_complete": fees_complete,
        "last_ts": last_ts,
        "net_pnl": None,
        "pnl_note": PNL_NOTE_SQLITE,
        "db_mtime": db_path.stat().st_mtime,
    }


# --------------------------------------------------------------------------
# CSV source (fallback: `hbot history` export or a trades CSV)
# --------------------------------------------------------------------------

CSV_PATTERNS = {
    "side": ["side", "trade_type", "tradetype", "direction", "type"],
    "price": ["price", "trade_price", "avg_price", "average_price"],
    "amount": ["amount", "quantity", "qty", "base_amount", "size"],
    "fee": ["fee_in_quote", "trade_fee_in_quote", "fees_usdt", "fee_usdt",
            "total_fees", "fees_paid", "fees", "fee"],
    "ts": ["timestamp", "created_at", "trade_time", "datetime", "date", "time"],
    "pnl": ["net_pnl_usdt", "net_pnl", "total_pnl", "realized_pnl", "pnl",
            "profit", "p&l"],
    "volume": ["volume_usdt", "quote_volume", "total_volume", "volume"],
    "trades": ["trade_count", "total_trades", "num_trades", "trades", "fills"],
    "buys": ["buy_count", "num_buys", "buys"],
    "sells": ["sell_count", "num_sells", "sells"],
}


def read_csv(csv_path: Path) -> dict:
    """Read a CSV export. Two shapes are supported:
      (a) per-trade rows (side + price + amount columns) — aggregates are
          computed row by row, like the SQLite path;
      (b) per-market summary rows (as printed by `hbot history`) — trades /
          volume / fees / PnL columns are summed across rows.
    An explicit PnL column, when present, is the only way this source yields
    net_pnl (it is Hummingbot's own reported number, not our invention)."""
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise LookupError("CSV has no header row")
        headers = [h for h in reader.fieldnames if h]
        col = {key: pick_column(headers, pats) for key, pats in CSV_PATTERNS.items()}
        rows = [r for r in reader if any((v or "").strip() for v in r.values())]

    stats = {
        "trades": 0, "buys": 0, "sells": 0, "unknown_side": 0,
        "volume": 0.0, "volume_seen": False,
        "fees": 0.0, "fees_seen": False, "fees_complete": True,
        "last_ts": None, "net_pnl": None,
        "pnl_note": PNL_NOTE_SQLITE.replace("trades DB", "CSV trade rows"),
        "db_mtime": csv_path.stat().st_mtime,
    }
    if not rows:
        return stats

    if col["price"] and col["amount"]:
        # (a) per-trade rows
        pnl_sum = 0.0
        pnl_seen = False
        for r in rows:
            stats["trades"] += 1
            price = fnum(r.get(col["price"]))
            amount = fnum(r.get(col["amount"]))
            side = classify_side(r.get(col["side"])) if col["side"] else None
            if side == "buy":
                stats["buys"] += 1
            elif side == "sell":
                stats["sells"] += 1
            else:
                stats["unknown_side"] += 1
            if price is not None and amount is not None:
                stats["volume"] += price * amount
                stats["volume_seen"] = True
            if col["fee"]:
                f = fnum(r.get(col["fee"]))
                if f is not None:
                    stats["fees"] += abs(f) if f else 0.0
                    stats["fees_seen"] = True
            if col["pnl"]:
                p = fnum(r.get(col["pnl"]))
                if p is not None:
                    pnl_sum += p
                    pnl_seen = True
            if col["ts"]:
                ts = parse_timestamp(r.get(col["ts"]))
                if ts is not None and (stats["last_ts"] is None or ts > stats["last_ts"]):
                    stats["last_ts"] = ts
        if stats["trades"] and stats["unknown_side"] == stats["trades"]:
            stats["buys"] = None
            stats["sells"] = None
        if pnl_seen:
            stats["net_pnl"] = pnl_sum
            stats["pnl_note"] = "Net PnL summed from the CSV's own PnL column (Hummingbot-reported)."
        return stats

    if col["volume"] or col["pnl"] or col["trades"]:
        # (b) summary rows (one per market)
        pnl_sum = 0.0
        pnl_seen = False
        buys_seen = sells_seen = False
        for r in rows:
            if col["trades"]:
                t = fnum(r.get(col["trades"]))
                if t is not None:
                    stats["trades"] += int(t)
            if col["volume"]:
                v = fnum(r.get(col["volume"]))
                if v is not None:
                    stats["volume"] += v
                    stats["volume_seen"] = True
            if col["fee"]:
                f = fnum(r.get(col["fee"]))
                if f is not None:
                    stats["fees"] += abs(f)
                    stats["fees_seen"] = True
            if col["pnl"]:
                p = fnum(r.get(col["pnl"]))
                if p is not None:
                    pnl_sum += p
                    pnl_seen = True
            if col["buys"]:
                b = fnum(r.get(col["buys"]))
                if b is not None:
                    stats["buys"] += int(b)
                    buys_seen = True
            if col["sells"]:
                s = fnum(r.get(col["sells"]))
                if s is not None:
                    stats["sells"] += int(s)
                    sells_seen = True
            if col["ts"]:
                ts = parse_timestamp(r.get(col["ts"]))
                if ts is not None and (stats["last_ts"] is None or ts > stats["last_ts"]):
                    stats["last_ts"] = ts
        if not buys_seen:
            stats["buys"] = None
        if not sells_seen:
            stats["sells"] = None
        if pnl_seen:
            stats["net_pnl"] = pnl_sum
            stats["pnl_note"] = "Net PnL taken from the `hbot history` CSV export (Hummingbot-reported)."
        return stats

    raise LookupError("CSV has neither per-trade (price+amount) nor summary (volume/pnl/trades) columns")


# --------------------------------------------------------------------------
# Payload assembly
# --------------------------------------------------------------------------

def base_payload(source_label: str) -> dict:
    return {
        "updated_at": iso_now(),
        "station": "no_data",
        "trades": None,
        "buys": None,
        "sells": None,
        "volume_usdt": None,
        "fees_usdt": None,
        "net_pnl_usdt": None,
        "pnl_note": PNL_NOTE_NO_DATA,
        "last_trade_at": None,
        "source": source_label,
        "bot_alive": False,
        "status_note": None,
    }


def build_payload(stats: dict | None, source_label: str, stale_seconds: float,
                  problem: str | None = None) -> dict:
    payload = base_payload(source_label)
    if stats is None:
        payload["status_note"] = problem or "Source unavailable."
        return payload

    payload["trades"] = stats["trades"]
    payload["buys"] = stats["buys"]
    payload["sells"] = stats["sells"]
    if not stats["trades"]:
        # Zero trades is a fact: no volume and no fees occurred.
        payload["volume_usdt"] = 0.0
        payload["fees_usdt"] = 0.0
    else:
        # With trades present, an unobserved total is null — never a
        # zero dressed up as data.
        payload["volume_usdt"] = (
            round(stats["volume"], 6) if stats.get("volume_seen") else None
        )
        payload["fees_usdt"] = (
            round(stats["fees"], 6) if stats.get("fees_seen") else None
        )
    payload["net_pnl_usdt"] = (
        round(stats["net_pnl"], 6) if stats["net_pnl"] is not None else None
    )
    payload["pnl_note"] = stats["pnl_note"]
    if stats["last_ts"] is not None:
        payload["last_trade_at"] = iso_from_epoch(stats["last_ts"])

    notes = []
    if stats["trades"] == 0:
        notes.append("Source found, but no filled trades recorded yet.")
    if stats.get("unknown_side"):
        if stats["buys"] is None:
            notes.append("No fill carried a readable buy/sell side, so the "
                         "buy/sell split is not reported.")
        else:
            notes.append(f"{stats['unknown_side']} fill(s) had an unreadable side and "
                         "count toward trades but not buys/sells.")
    if stats["trades"] and payload["volume_usdt"] is None:
        notes.append("Fills lacked usable price/amount data, so volume is not reported.")
    if stats["trades"] and payload["fees_usdt"] is None:
        notes.append("The source carried no fee data, so fees are not reported "
                     "(not assumed to be zero).")
    if stats["trades"] and not stats.get("fees_complete", True):
        notes.append("Some fees were charged in a non-quote token and could not be "
                     "valued in USDT; fees_usdt is a lower bound.")

    # Freshness: the bot is considered alive if the newest evidence (last
    # trade or source-file write) is inside the stale window.
    freshest = max(stats.get("db_mtime") or 0, stats.get("last_ts") or 0)
    age = time.time() - freshest if freshest else None
    alive = bool(stats["trades"]) and age is not None and age <= stale_seconds
    payload["bot_alive"] = alive
    if stats["trades"]:
        if alive:
            payload["station"] = "online"
        else:
            payload["station"] = "no_data"
            notes.append(
                "Trades exist but nothing new within "
                f"{int(stale_seconds // 60)} min — the bot may be stopped or the "
                "Mac asleep. Numbers shown are the last recorded state."
            )
    payload["status_note"] = " ".join(notes) if notes else None
    return payload


# --------------------------------------------------------------------------
# Source resolution / emission
# --------------------------------------------------------------------------

def find_csv_near(db_path: Path) -> Path | None:
    folder = db_path.parent
    if not folder.is_dir():
        return None
    csvs = sorted(folder.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    for p in csvs:
        n = p.name.lower()
        if "trade" in n or "histor" in n:
            return p
    return csvs[0] if csvs else None


def resolve_source(args) -> tuple[str, Path | None, str]:
    """Returns (kind, path, label). kind is 'sqlite', 'csv', or 'none'."""
    db = Path(args.db)
    if args.source == "sqlite":
        return "sqlite", db, f"sqlite:{db}"
    if args.source == "csv":
        csv_path = Path(args.csv) if args.csv else find_csv_near(db)
        if csv_path is None:
            return "none", None, "none"
        return "csv", csv_path, f"csv:{csv_path}"
    # auto: SQLite first, CSV second, nothing last.
    if db.is_file():
        return "sqlite", db, f"sqlite:{db}"
    csv_path = Path(args.csv) if args.csv else find_csv_near(db)
    if csv_path is not None:
        return "csv", csv_path, f"csv:{csv_path}"
    return "none", None, "none"


def collect(args) -> dict:
    kind, path, label = resolve_source(args)
    stale = args.stale_minutes * 60.0
    if kind == "none":
        return build_payload(None, "none", stale,
                             problem="No Hummingbot data source found "
                                     f"(looked for {args.db} and nearby CSVs). "
                                     "Bot not installed yet, or data/ path differs.")
    try:
        stats = read_sqlite(path) if kind == "sqlite" else read_csv(path)
    except (FileNotFoundError, LookupError, sqlite3.Error, OSError) as exc:
        return build_payload(None, label, stale, problem=f"Source unreadable: {exc}")
    return build_payload(stats, label, stale)


def write_json(payload: dict, out_path: Path) -> None:
    """Atomic write: the dashboard must never read a half-written file."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(out_path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, out_path)


def push(payload: dict, url: str) -> bool:
    """POST the JSON to the backend endpoint. The shared secret comes from
    the HB_SYNC_SECRET env var; without it we do not POST at all (the file
    write is the fallback transport)."""
    secret = os.environ.get("HB_SYNC_SECRET")
    if not secret:
        print("sync_hummingbot: --push-url given but HB_SYNC_SECRET is unset; "
              "writing the file only.", file=sys.stderr)
        return False
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {secret}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 200 <= resp.status < 300
    except (urllib.error.URLError, OSError) as exc:
        print(f"sync_hummingbot: push failed ({exc}); file write still stands.",
              file=sys.stderr)
        return False


def emit(args) -> dict:
    payload = collect(args)
    write_json(payload, Path(args.out))
    if args.push_url:
        push(payload, args.push_url)
    return payload


def source_signature(args) -> tuple:
    """(kind, path, mtime, wal_mtime) — changes whenever the source does.
    For SQLite the -wal sidecar is watched too: with WAL journaling, fresh
    commits land in the -wal file before the main DB file is touched."""
    kind, path, _ = resolve_source(args)
    if path is None or not path.exists():
        return (kind, str(path), None, None)
    wal = Path(str(path) + "-wal")
    return (
        kind,
        str(path),
        path.stat().st_mtime,
        wal.stat().st_mtime if wal.exists() else None,
    )


def watch(args) -> None:
    print(f"sync_hummingbot: watching (interval {args.interval}s). Ctrl-C to stop.")
    last_sig = None
    try:
        while True:
            sig = source_signature(args)
            if sig != last_sig:
                payload = emit(args)
                print(f"[{iso_now()}] emitted: station={payload['station']} "
                      f"trades={payload['trades']} source={payload['source']}")
                last_sig = sig
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("sync_hummingbot: stopped.")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--source", choices=["auto", "sqlite", "csv"], default="auto",
                    help="Where to read history from (default: auto = SQLite, then CSV).")
    ap.add_argument("--db", default=str(DEFAULT_DB),
                    help=f"Path to the Hummingbot SQLite trades DB (default: {DEFAULT_DB}).")
    ap.add_argument("--csv", default=None,
                    help="Path to a CSV history export (default: newest trades/history CSV "
                         "next to the DB).")
    ap.add_argument("--out", default=str(DEFAULT_OUT),
                    help=f"Output JSON path (default: {DEFAULT_OUT}).")
    ap.add_argument("--watch", action="store_true",
                    help="Keep running; re-emit whenever the source file changes "
                         "(this is what makes the dashboard update per paper trade).")
    ap.add_argument("--interval", type=float, default=DEFAULT_WATCH_INTERVAL,
                    help=f"Watch poll interval in seconds (default: {DEFAULT_WATCH_INTERVAL}).")
    ap.add_argument("--stale-minutes", type=float, default=DEFAULT_STALE_MINUTES,
                    help="No new activity within this window => station reported as "
                         f"no_data / bot not alive (default: {DEFAULT_STALE_MINUTES}).")
    ap.add_argument("--push-url", default=None,
                    help="Optional backend endpoint to POST the JSON to. Requires the "
                         "HB_SYNC_SECRET env var; without it, only the file is written.")
    args = ap.parse_args(argv)

    if args.watch:
        watch(args)
    else:
        payload = emit(args)
        print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
