"""Shared helpers for the daily pipeline modules.

Small, dependency-light utilities that several pipeline scripts need (date
resolution, the daily-log CSV schema, and the append/de-dup write used for the
dated log files). Kept free of model/Betfair/Discord imports so importing it is
cheap and never risks a circular import.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

_RACING_FRACTIONS = [
    (0.1, "1/10"), (0.2, "1/5"), (0.222, "2/9"), (0.25, "1/4"),
    (0.286, "2/7"), (0.3, "3/10"), (0.333, "1/3"), (0.364, "4/11"),
    (0.4, "2/5"), (0.444, "4/9"), (0.5, "1/2"), (0.533, "8/15"),
    (0.571, "4/7"), (0.615, "8/13"), (0.667, "4/6"), (0.727, "8/11"),
    (0.8, "4/5"), (0.833, "5/6"), (0.909, "10/11"), (1.0, "EVS"),
    (1.1, "11/10"), (1.2, "6/5"), (1.25, "5/4"), (1.375, "11/8"),
    (1.5, "6/4"), (1.625, "13/8"), (1.75, "7/4"), (1.875, "15/8"),
    (2.0, "2/1"), (2.25, "9/4"), (2.5, "5/2"), (2.75, "11/4"),
    (3.0, "3/1"), (3.333, "100/30"), (3.5, "7/2"), (4.0, "4/1"),
    (4.5, "9/2"), (5.0, "5/1"), (5.5, "11/2"), (6.0, "6/1"),
    (6.5, "13/2"), (7.0, "7/1"), (7.5, "15/2"), (8.0, "8/1"),
    (9.0, "9/1"), (10.0, "10/1"), (11.0, "11/1"), (12.0, "12/1"),
    (14.0, "14/1"), (16.0, "16/1"), (20.0, "20/1"), (22.0, "22/1"),
    (25.0, "25/1"), (28.0, "28/1"), (33.0, "33/1"), (40.0, "40/1"),
    (50.0, "50/1"), (66.0, "66/1"), (80.0, "80/1"), (100.0, "100/1"),
]


def decimal_to_fractional(odds: float) -> str:
    """Convert decimal odds (e.g. 3.75) to standard racing fractional (e.g. '11/4')."""
    if odds <= 1.0:
        return "1/1"
    profit = odds - 1.0
    best_frac = min(_RACING_FRACTIONS, key=lambda x: abs(x[0] - profit))
    return best_frac[1]


BETS_LOG_COLS = [
    "date", "race_id", "runner_id", "horse", "course", "time",
    "category", "model_prob", "back_odds", "edge", "stake", "model_signals",
    "forecast_odds",
]


def resolve_date(date_str: str) -> date:
    """Resolve a CLI date argument to a date.

    Accepts the relative keywords 'today', 'tomorrow' and 'yesterday', or an
    explicit YYYY-MM-DD string.
    """
    if date_str == "today":
        return date.today()
    if date_str == "tomorrow":
        return date.today() + timedelta(days=1)
    if date_str == "yesterday":
        return date.today() - timedelta(days=1)
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def append_dated_csv(
    log_path: Path,
    new_rows: pd.DataFrame,
    target_date: date,
    cols: list[str],
    refresh: bool = False,
    label: str = "rows",
) -> bool:
    """Append new_rows to a CSV keyed by a 'date' column, de-duplicating by date.

    Pandas-based so the schema auto-migrates: reindexing to `cols` backfills any
    columns missing from an older CSV rather than corrupting on append. A date
    already present is skipped unless refresh=True, which replaces that date's
    rows. `label` is only used in the printed status lines. Returns True if it
    wrote, False if it skipped.
    """
    if new_rows.empty:
        return False
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    if log_path.exists():
        existing = pd.read_csv(log_path)
        already = (pd.to_datetime(existing["date"]).dt.date == target_date).any()
        if already and not refresh:
            print(f"  {label.capitalize()} for {target_date} already logged (skipping)", flush=True)
            return False
        if already and refresh:
            existing = existing[pd.to_datetime(existing["date"]).dt.date != target_date]
            print(f"  Refreshing {label} for {target_date}", flush=True)
        combined = pd.concat([existing, new_rows], ignore_index=True)
    else:
        combined = new_rows

    combined.reindex(columns=cols).to_csv(log_path, index=False)
    print(f"  Logged {len(new_rows)} {label} to {log_path}", flush=True)
    return True
