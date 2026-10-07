from __future__ import annotations

import os
import sys
import time
from datetime import date
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ingestion.db_connect import get_db
from quality.checks import run_all_checks
from quality.leakage_guard import check_no_leakage

DB_PATH = Path(os.environ.get("RACING_DB", str(ROOT / "racing.duckdb")))
SQL_DIR = ROOT / "sql" / "features"
BASELINE_MIN_RACE_DATE = date(2015, 1, 1)

PHASE_FILES: list[tuple[str, str]] = [
    ("001_horse_form.sql", "f001"),
    ("002_draw_bias.sql", "f002"),
    ("003_trainer_stats.sql", "f003"),
    ("004_jockey_stats.sql", "f004"),
    ("005_class_features.sql", "f005"),
    ("006_race_context.sql", "f006"),
    ("007_collateral_form.sql", "f007"),
    ("008_runner_profile.sql", "f008"),
    ("009_speed_and_changes.sql", "f009"),
    ("010_rating_trajectory.sql", "f010"),
]


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _column_null_rates(con: duckdb.DuckDBPyConnection, table_name: str) -> list[tuple[str, float]]:
    cols = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        [table_name],
    ).fetchall()

    total = int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
    if total == 0:
        return [(c[0], 0.0) for c in cols]

    out: list[tuple[str, float]] = []
    for (col_name,) in cols:
        qcol = _quote_ident(col_name)
        null_count = int(con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {qcol} IS NULL").fetchone()[0])
        out.append((col_name, null_count / total))
    return out


def _prepare_upstream_inputs(
    con: duckdb.DuckDBPyConnection,
    scope_start: date | None = None,
    affected_race_ids: set[str] | None = None,
) -> None:
    scope_clause = ""
    scope_clause2 = ""
    scope_clause_hh = ""
    scope_params: list[date] = []
    race_scope_params: list[date] = []
    race_scope_clause = ""
    if scope_start is not None:
        scope_clause = " AND ra.race_date >= ?"
        scope_clause2 = " AND ra2.race_date >= ?"
        scope_clause_hh = (
            " AND EXISTS ("
            "SELECT 1 FROM races scope_ra "
            "WHERE scope_ra.race_id = hh.race_id AND scope_ra.race_date >= ?)"
        )
        scope_params = [scope_start]
    if affected_race_ids is not None:
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE _affected_race_ids (race_id VARCHAR PRIMARY KEY)
            """
        )
        con.executemany(
            "INSERT INTO _affected_race_ids VALUES (?)",
            [(race_id,) for race_id in sorted(affected_race_ids)],
        )
        race_scope_clause = (
            " AND EXISTS ("
            "SELECT 1 FROM _affected_race_ids affected "
            "WHERE affected.race_id = ra.race_id)"
        )
        scope_clause_hh = (
            " AND EXISTS ("
            "SELECT 1 FROM _affected_race_ids affected "
            "WHERE affected.race_id = hh.race_id)"
        )
        race_scope_params = []
    else:
        race_scope_params = scope_params
    for table, col, col_type in [
        ("horse_history", "non_completion", "VARCHAR"),
        ("results", "non_completion", "VARCHAR"),
        ("runners", "sex", "VARCHAR"),
        ("races", "course_key", "VARCHAR"),
        ("runners", "trainer_name_norm", "VARCHAR"),
        ("runners", "jockey_name_norm", "VARCHAR"),
        ("trainer_history", "trainer_name_norm", "VARCHAR"),
        ("jockey_history", "jockey_name_norm", "VARCHAR"),
    ]:
        existing = {r[0] for r in con.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}'"
        ).fetchall()}
        if col not in existing:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")

    con.execute(
        """
        UPDATE races
        SET course_key = lower(normalise_course(course_name))
        WHERE course_key IS NULL
        """
    )
    con.execute(
        """
        UPDATE runners
        SET
            trainer_name_norm = COALESCE(NULLIF(TRIM(trainer_name), ''), 'Unknown'),
            jockey_name_norm = COALESCE(NULLIF(TRIM(jockey_name), ''), 'Unknown')
        WHERE trainer_name_norm IS NULL OR jockey_name_norm IS NULL
        """
    )
    con.execute(
        """
        UPDATE trainer_history
        SET trainer_name_norm = COALESCE(NULLIF(TRIM(trainer_name), ''), 'Unknown')
        WHERE trainer_name_norm IS NULL
        """
    )
    con.execute(
        """
        UPDATE jockey_history
        SET jockey_name_norm = COALESCE(NULLIF(TRIM(jockey_name), ''), 'Unknown')
        WHERE jockey_name_norm IS NULL
        """
    )

    con.execute(
        f"""
        UPDATE horse_history hh
        SET
            going_code = COALESCE(hh.going_code, ra.going_code),
            distance_furlongs = COALESCE(hh.distance_furlongs, ra.distance_furlongs),
            race_class = COALESCE(hh.race_class, ra.race_class),
            is_handicap = COALESCE(hh.is_handicap, ra.is_handicap),
            field_size = COALESCE(hh.field_size, ra.field_size)
        FROM races ra
        WHERE hh.race_id = ra.race_id
        {scope_clause if affected_race_ids is None else race_scope_clause}
        """,
        race_scope_params,
    )

    con.execute(
        f"""
        UPDATE horse_history hh
        SET
            finishing_position = COALESCE(hh.finishing_position, res.finishing_position),
            won = COALESCE(hh.won, res.won),
            btn_lengths = COALESCE(hh.btn_lengths, res.btn_lengths),
            rpr = COALESCE(hh.rpr, res.rpr),
            non_completion = COALESCE(hh.non_completion, res.non_completion)
        FROM results res
        JOIN runners ru ON res.runner_id = ru.runner_id
        WHERE ru.horse_id = hh.horse_id
          AND res.race_id = hh.race_id
          {scope_clause_hh}
        """,
        race_scope_params,
    )

    con.execute(
        f"""
        UPDATE horse_history hh
        SET headgear = COALESCE(hh.headgear, ru.headgear)
        FROM runners ru
        WHERE ru.horse_id = hh.horse_id
          AND ru.race_id = hh.race_id
          {scope_clause_hh}
        """,
        race_scope_params,
    )

    con.execute(
        f"""
        UPDATE horse_history hh
        SET days_since_prev_run = sub.days_since_prev_run
        FROM (
            SELECT
                history_id,
                DATE_DIFF(
                    'day',
                    LAG(scheduled_off_utc) OVER (PARTITION BY horse_id ORDER BY scheduled_off_utc),
                    scheduled_off_utc
                ) AS days_since_prev_run
            FROM horse_history
            WHERE horse_id IN (
                SELECT DISTINCT hh2.horse_id
                FROM horse_history hh2
                JOIN races ra2 ON ra2.race_id = hh2.race_id
                WHERE TRUE {scope_clause2}
            )
        ) sub
        WHERE hh.history_id = sub.history_id
        """,
        scope_params,
    )

    con.execute(
        f"""
        INSERT INTO trainer_history (
            history_id,
            trainer_id,
            trainer_name,
            race_id,
            race_date,
            scheduled_off_utc,
            course_id,
            race_type,
            going_code,
            distance_furlongs,
            race_class,
            days_since_last_run,
            won,
            finishing_position,
            field_size,
            event_timestamp_utc,
            decision_cutoff_utc,
            ingest_timestamp_utc,
            trainer_name_norm
        )
        SELECT
            r.runner_id || '_tr' AS history_id,
            COALESCE(r.trainer_id, 'unknown') AS trainer_id,
            COALESCE(NULLIF(TRIM(r.trainer_name), ''), 'unknown') AS trainer_name,
            r.race_id,
            ra.race_date,
            ra.scheduled_off_utc,
            ra.course_id,
            ra.race_type,
            ra.going_code,
            ra.distance_furlongs,
            ra.race_class,
            hh.days_since_prev_run AS days_since_last_run,
            res.won,
            res.finishing_position,
            ra.field_size,
            ra.scheduled_off_utc AS event_timestamp_utc,
            ra.decision_cutoff_utc,
            NOW() AS ingest_timestamp_utc,
            COALESCE(NULLIF(TRIM(r.trainer_name), ''), 'Unknown')
        FROM runners r
        JOIN races ra ON r.race_id = ra.race_id
        JOIN results res ON r.runner_id = res.runner_id
        LEFT JOIN horse_history hh ON hh.race_id = r.race_id AND hh.horse_id = r.horse_id
        WHERE COALESCE(NULLIF(TRIM(r.trainer_name), ''), '') <> ''
          {scope_clause if affected_race_ids is None else race_scope_clause}
        ON CONFLICT (history_id) DO UPDATE SET
            trainer_id = excluded.trainer_id,
            trainer_name = excluded.trainer_name,
            days_since_last_run = excluded.days_since_last_run,
            won = excluded.won,
            finishing_position = excluded.finishing_position,
            field_size = excluded.field_size,
            trainer_name_norm = excluded.trainer_name_norm
        """,
        race_scope_params,
    )

    con.execute(
        f"""
        INSERT INTO jockey_history (
            history_id,
            jockey_id,
            jockey_name,
            trainer_id,
            race_id,
            race_date,
            scheduled_off_utc,
            course_id,
            race_type,
            going_code,
            won,
            finishing_position,
            field_size,
            event_timestamp_utc,
            decision_cutoff_utc,
            ingest_timestamp_utc,
            jockey_name_norm
        )
        SELECT
            r.runner_id || '_jk' AS history_id,
            COALESCE(r.jockey_id, 'unknown') AS jockey_id,
            COALESCE(NULLIF(TRIM(r.jockey_name), ''), 'unknown') AS jockey_name,
            COALESCE(r.trainer_id, 'unknown') AS trainer_id,
            r.race_id,
            ra.race_date,
            ra.scheduled_off_utc,
            ra.course_id,
            ra.race_type,
            ra.going_code,
            res.won,
            res.finishing_position,
            ra.field_size,
            ra.scheduled_off_utc AS event_timestamp_utc,
            ra.decision_cutoff_utc,
            NOW() AS ingest_timestamp_utc,
            COALESCE(NULLIF(TRIM(r.jockey_name), ''), 'Unknown')
        FROM runners r
        JOIN races ra ON r.race_id = ra.race_id
        JOIN results res ON r.runner_id = res.runner_id
        WHERE COALESCE(NULLIF(TRIM(r.jockey_name), ''), '') <> ''
          {scope_clause if affected_race_ids is None else race_scope_clause}
        ON CONFLICT (history_id) DO UPDATE SET
            jockey_id = excluded.jockey_id,
            jockey_name = excluded.jockey_name,
            trainer_id = excluded.trainer_id,
            won = excluded.won,
            finishing_position = excluded.finishing_position,
            field_size = excluded.field_size,
            jockey_name_norm = excluded.jockey_name_norm
        """,
        race_scope_params,
    )


def _materialize_feature_store(
    con: duckdb.DuckDBPyConnection,
    output_table: str = "feature_store",
    race_date: date | None = None,
) -> int:
    output_ident = _quote_ident(output_table)
    table_kind = "TEMP " if output_table != "feature_store" else ""
    date_clause = ""
    date_params: list[date] = []
    if race_date is not None:
        date_clause = " AND ra.race_date IN (SELECT race_date FROM _feature_scope_dates)"
    con.execute(
        f"""
        CREATE OR REPLACE {table_kind}TABLE {output_ident} AS
        WITH base AS (
            SELECT
                r.runner_id,
                r.race_id,
                ra.race_date,
                ra.decision_cutoff_utc,
                res.won AS target,
                f001.horse_runs_last_3_positions,
                f001.horse_runs_last_5_positions,
                f001.horse_wins_last_5,
                f001.horse_win_rate_last_10,
                f001.horse_days_since_last_run,
                f001.horse_runs_last_90_days,
                f001.horse_going_group_affinity,
                f001.horse_going_group_place_rate,
                f001.horse_distance_affinity,
                f001.horse_distance_place_rate,
                f001.horse_course_affinity,
                f001.horse_course_place_rate,
                f001.horse_course_runs,
                f001.horse_weighted_form,
                f001.horse_place_rate_last_5,
                f001.horse_place_rate_last_10,
                f001.horse_improvement_index,
                f001.horse_avg_position_pct_last_5,
                f001.horse_best_rpr_last_5,
                f001.horse_best_rpr_rp_last_5,
                f001.horse_avg_rpr_last_3,
                f001.horse_last_rpr,
                f001.horse_avg_class_last_3,
                f005.horse_class_delta AS horse_class_delta,
                f001.horse_form_trend,
                f001.horse_first_time_headgear,
                f001.horse_pu_rate,
                f001.horse_nc_last_5,
                f002.draw_position,
                f002.draw_field_percentile,
                f002.draw_course_going_win_rate,
                f002.draw_bias_coefficient,
                f002.draw_is_null,
                f003.trainer_win_rate_90d,
                f003.trainer_win_rate_course_90d,
                f003.trainer_course_going_win_rate,
                f003.trainer_dist_alltime_win_rate,
                f003.trainer_win_rate_going_90d,
                f003.trainer_win_rate_dist_band_90d,
                f003.trainer_runs_90d,
                f003.trainer_fresh_win_rate,
                f003.trainer_fresh_runs,
                f004.jockey_win_rate_90d,
                f004.jockey_win_rate_course_90d,
                f004.jockey_dist_win_rate_90d,
                f004.jockey_trainer_combo_win_rate,
                f004.jockey_trainer_combo_runs,
                f004.jockey_runs_90d,
                f005.race_class_encoded,
                COALESCE(f005.is_handicap, FALSE) AS is_handicap,
                f005.race_grade,
                f005.prize_money_log,
                f005.is_class_dropper,
                f006.field_size,
                f006.pace_front_runners,
                f006.pace_hold_up_horses,
                f006.pace_pressure_index,
                f006.surface_encoded,
                f006.going_encoded,
                f006.race_type_encoded,
                f006.race_month,
                f006.race_day_of_week,
                f007.collateral_beaten_win_rate,
                f007.collateral_beaten_place_rate,
                f007.collateral_franked_winners,
                f007.collateral_beaten_count,
                f008.weight_lbs,
                f008.weight_vs_top,
                f008.weight_vs_field_avg,
                f008.horse_age,
                f008.runner_official_rating,
                f008.rating_vs_top,
                f008.rating_vs_field_avg,
                f008.field_avg_rating,
                f008.career_runs,
                f008.career_win_rate,
                f008.career_place_rate,
                f008.position_consistency,
                f009.avg_speed_last_3,
                f009.best_speed_last_5,
                f009.last_run_speed,
                f009.is_jumps,
                f009.trip_change_furlongs,
                f009.weight_change_lbs,
                f009.last_run_btn_lengths,
                f009.avg_btn_last_3,
                f009.jockey_upgrade_signal,
                f009.trainer_win_rate_14d,
                f009.trainer_runs_14d,
                f010.last_win_official_rating,
                f010.best_win_official_rating,
                f010.peak_rating_since_last_win,
                f010.rating_vs_last_win_mark,
                f010.rating_vs_best_win_mark,
                f010.rating_drop_from_post_win_peak,
                f010.rating_rise_after_last_win,
                f010.days_since_last_win,
                CASE
                    WHEN UPPER(r.sex) = 'G' THEN 0
                    WHEN UPPER(r.sex) = 'M' THEN 1
                    WHEN UPPER(r.sex) = 'F' THEN 2
                    WHEN UPPER(r.sex) = 'C' THEN 3
                    WHEN UPPER(r.sex) = 'H' THEN 4
                    ELSE NULL
                END AS sex_encoded,
                CASE WHEN UPPER(r.sex) IN ('F', 'M') THEN 1 ELSE 0 END AS is_female,
                ra.surface AS race_surface,
                ra.race_type AS race_type_raw,
                COALESCE(f006.distance_furlongs, ra.distance_furlongs) AS distance_raw
            FROM runners r
            JOIN races ra ON r.race_id = ra.race_id
            JOIN results res ON r.runner_id = res.runner_id
            LEFT JOIN f001 ON r.runner_id = f001.runner_id
            LEFT JOIN f002 ON r.runner_id = f002.runner_id
            LEFT JOIN f003 ON r.runner_id = f003.runner_id
            LEFT JOIN f004 ON r.runner_id = f004.runner_id
            LEFT JOIN f005 ON r.runner_id = f005.runner_id
            LEFT JOIN f006 ON r.runner_id = f006.runner_id
            LEFT JOIN f007 ON r.runner_id = f007.runner_id
            LEFT JOIN f008 ON r.runner_id = f008.runner_id
            LEFT JOIN f009 ON r.runner_id = f009.runner_id
            LEFT JOIN f010 ON r.runner_id = f010.runner_id
            WHERE ra.is_standard_race = TRUE
              {date_clause}
        ),
        parsed AS (
            SELECT
                b.*,
                CASE
                    WHEN b.distance_raw IS NOT NULL THEN b.distance_raw
                    WHEN NULLIF(regexp_extract(LOWER(COALESCE(b.race_type_raw, '')), '([0-9]+)m', 1), '') IS NOT NULL THEN
                        CAST(NULLIF(regexp_extract(LOWER(COALESCE(b.race_type_raw, '')), '([0-9]+)m', 1), '') AS DOUBLE) * 8.0
                        + COALESCE(CAST(NULLIF(regexp_extract(LOWER(COALESCE(b.race_type_raw, '')), '([0-9]+)f', 1), '') AS DOUBLE), 0.0)
                    WHEN NULLIF(regexp_extract(LOWER(COALESCE(b.race_type_raw, '')), '([0-9]+)f', 1), '') IS NOT NULL THEN
                        CAST(NULLIF(regexp_extract(LOWER(COALESCE(b.race_type_raw, '')), '([0-9]+)f', 1), '') AS DOUBLE)
                    ELSE NULL
                END AS distance_after_parse
            FROM base b
        ),
        medians AS (
            SELECT
                race_surface,
                race_type_raw,
                MEDIAN(distance_after_parse) AS median_distance
            FROM parsed
            WHERE distance_after_parse IS NOT NULL
            GROUP BY 1, 2
        ),
        global_median AS (
            SELECT MEDIAN(distance_after_parse) AS median_distance
            FROM parsed
            WHERE distance_after_parse IS NOT NULL
        )
        SELECT
            p.runner_id,
            p.race_id,
            p.race_date,
            p.decision_cutoff_utc,
            p.target,
            p.horse_runs_last_3_positions,
            p.horse_runs_last_5_positions,
            p.horse_wins_last_5,
            p.horse_win_rate_last_10,
            p.horse_days_since_last_run,
            p.horse_runs_last_90_days,
            COALESCE(p.horse_going_group_affinity) AS horse_going_group_affinity,
            COALESCE(p.horse_going_group_place_rate) AS horse_going_group_place_rate,
            p.horse_distance_affinity,
            p.horse_distance_place_rate,
            p.horse_course_affinity,
            p.horse_course_place_rate,
            p.horse_course_runs,
            p.horse_weighted_form,
            p.horse_place_rate_last_5,
            p.horse_place_rate_last_10,
            p.horse_improvement_index,
            p.horse_avg_position_pct_last_5,
            p.horse_best_rpr_last_5,
            p.horse_best_rpr_rp_last_5,
            p.horse_avg_rpr_last_3,
            p.horse_last_rpr,
            p.horse_avg_class_last_3,
            p.horse_class_delta,
            p.horse_form_trend,
            p.horse_first_time_headgear,
            p.horse_pu_rate,
            p.horse_nc_last_5,
            p.draw_position,
            p.draw_field_percentile,
            p.draw_course_going_win_rate,
            p.draw_bias_coefficient,
            p.draw_is_null,
            p.trainer_win_rate_90d,
            p.trainer_win_rate_course_90d,
            p.trainer_course_going_win_rate,
            p.trainer_dist_alltime_win_rate,
            p.trainer_win_rate_going_90d,
            p.trainer_win_rate_dist_band_90d,
            p.trainer_runs_90d,
            p.trainer_fresh_win_rate,
            p.trainer_fresh_runs,
            p.jockey_win_rate_90d,
            p.jockey_win_rate_course_90d,
            p.jockey_dist_win_rate_90d,
            p.jockey_trainer_combo_win_rate,
            p.jockey_trainer_combo_runs,
            p.jockey_runs_90d,
            p.race_class_encoded,
            p.is_handicap,
            p.race_grade,
            p.prize_money_log,
            p.is_class_dropper,
            p.field_size,
            p.pace_front_runners,
            p.pace_hold_up_horses,
            p.pace_pressure_index,
            p.surface_encoded,
            LEAST(
                COALESCE(p.distance_after_parse, m.median_distance, g.median_distance),
                36.0
            ) AS distance_furlongs,
            p.going_encoded,
            p.race_type_encoded,
            p.race_month,
            p.race_day_of_week,
            p.collateral_beaten_win_rate,
            p.collateral_beaten_place_rate,
            p.collateral_franked_winners,
            p.collateral_beaten_count,
            p.weight_lbs,
            p.weight_vs_top,
            p.weight_vs_field_avg,
            p.horse_age,
            p.runner_official_rating,
            p.rating_vs_top,
            p.rating_vs_field_avg,
            p.field_avg_rating,
            p.career_runs,
            p.career_win_rate,
            p.career_place_rate,
            p.position_consistency,
            p.avg_speed_last_3,
            p.best_speed_last_5,
            p.last_run_speed,
            p.is_jumps,
            p.trip_change_furlongs,
            p.weight_change_lbs,
            p.last_run_btn_lengths,
            p.avg_btn_last_3,
            p.jockey_upgrade_signal,
            p.trainer_win_rate_14d,
            p.trainer_runs_14d,
            CASE WHEN p.is_handicap THEN p.last_win_official_rating ELSE NULL END AS last_win_official_rating,
            CASE WHEN p.is_handicap THEN p.best_win_official_rating ELSE NULL END AS best_win_official_rating,
            CASE WHEN p.is_handicap THEN p.peak_rating_since_last_win ELSE NULL END AS peak_rating_since_last_win,
            CASE WHEN p.is_handicap THEN p.rating_vs_last_win_mark ELSE NULL END AS rating_vs_last_win_mark,
            CASE WHEN p.is_handicap THEN p.rating_vs_best_win_mark ELSE NULL END AS rating_vs_best_win_mark,
            CASE WHEN p.is_handicap THEN p.rating_drop_from_post_win_peak ELSE NULL END AS rating_drop_from_post_win_peak,
            CASE WHEN p.is_handicap THEN p.rating_rise_after_last_win ELSE NULL END AS rating_rise_after_last_win,
            CASE WHEN p.is_handicap THEN p.days_since_last_win ELSE NULL END AS days_since_last_win,
            p.sex_encoded,
            p.is_female
        FROM parsed p
        LEFT JOIN medians m
            ON COALESCE(m.race_surface, '') = COALESCE(p.race_surface, '')
           AND COALESCE(m.race_type_raw, '') = COALESCE(p.race_type_raw, '')
        CROSS JOIN global_median g
        """,
        date_params,
    )
    return int(con.execute(f"SELECT COUNT(*) FROM {output_ident}").fetchone()[0])


def rebuild_feature_window(
    con: duckdb.DuckDBPyConnection,
    target_date: date,
    lookback_days: int = 14,
) -> int:
    """Rebuild and merge only the recent feature window used by daily scoring."""
    from datetime import timedelta

    scope_start = target_date - timedelta(days=lookback_days)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_changed_races (
            race_id VARCHAR PRIMARY KEY,
            changed_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    affected_rows = con.execute(
        """
        SELECT race_id
        FROM races
        WHERE race_date = ?
        UNION
        SELECT changed.race_id
        FROM ingestion_changed_races changed
        JOIN races ra ON ra.race_id = changed.race_id
        WHERE ra.race_date >= ? AND ra.race_date <= ?
        """,
        [target_date, scope_start, target_date],
    ).fetchall()
    affected_race_ids = {row[0] for row in affected_rows}
    prepare_started = time.perf_counter()
    _prepare_upstream_inputs(
        con,
        scope_start=scope_start,
        affected_race_ids=affected_race_ids,
    )
    print(
        f"FEATURE_STAGE=upstream_inputs "
        f"ELAPSED_SECONDS={time.perf_counter() - prepare_started:.3f}",
        flush=True,
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _feature_scope_dates AS
        SELECT DISTINCT race_date
        FROM races
        WHERE race_date >= ? AND race_date <= ?
        """,
        [scope_start, target_date],
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _source_races AS
        SELECT * FROM races
        WHERE race_date IN (SELECT race_date FROM _feature_scope_dates)
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _source_runners AS
        SELECT ru.* FROM runners ru
        JOIN _source_races ra ON ra.race_id = ru.race_id
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _source_results AS
        SELECT res.* FROM results res
        JOIN _source_races ra ON ra.race_id = res.race_id
        """
    )
    con.execute("CREATE OR REPLACE TEMP TABLE races AS SELECT * FROM _source_races")
    con.execute("CREATE OR REPLACE TEMP TABLE runners AS SELECT * FROM _source_runners")
    con.execute("CREATE OR REPLACE TEMP TABLE results AS SELECT * FROM _source_results")
    for sql_name, table_name in PHASE_FILES:
        phase_started = time.perf_counter()
        sql_text = (SQL_DIR / sql_name).read_text(encoding="utf-8")
        sql_text = sql_text.replace(
            f"CREATE OR REPLACE TABLE {table_name}",
            f"CREATE OR REPLACE TEMP TABLE {table_name}",
            1,
        )
        con.execute(sql_text)
        print(
            f"FEATURE_PHASE={sql_name} "
            f"ELAPSED_SECONDS={time.perf_counter() - phase_started:.3f}",
            flush=True,
        )

    materialize_started = time.perf_counter()
    rows = _materialize_feature_store(
        con,
        output_table="_feature_store_window",
        race_date=target_date,
    )
    print(
        f"FEATURE_STAGE=materialize_and_merge "
        f"ELAPSED_SECONDS={time.perf_counter() - materialize_started:.3f}",
        flush=True,
    )
    con.execute(
        """
        DELETE FROM feature_store
        WHERE race_date IN (SELECT race_date FROM _feature_scope_dates)
        """
    )
    con.execute(
        """
        INSERT INTO feature_store
        SELECT * FROM _feature_store_window
        """
    )
    if affected_race_ids:
        con.execute(
            """
            DELETE FROM ingestion_changed_races
            WHERE race_id IN (SELECT race_id FROM _affected_race_ids)
            """
        )
    return rows


def main() -> None:
    con = get_db(DB_PATH)
    try:
        _prepare_upstream_inputs(con)
        for sql_name, table_name in PHASE_FILES:
            phase_started = time.perf_counter()
            sql_path = SQL_DIR / sql_name
            con.execute(sql_path.read_text(encoding="utf-8"))
            print(
                f"FEATURE_PHASE={sql_name} "
                f"ELAPSED_SECONDS={time.perf_counter() - phase_started:.3f}",
                flush=True,
            )
            row_count = int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
            null_rates = _column_null_rates(con, table_name)
            print(f"PHASE2_FILE={sql_name}")
            print(f"PHASE2_TABLE={table_name}")
            print(f"PHASE2_ROWS={row_count}")
            print("PHASE2_NULL_RATES_START")
            for col_name, rate in null_rates:
                print(f"{col_name}|{rate:.6f}")
            print("PHASE2_NULL_RATES_END")

            leak_result = check_no_leakage(table_name=table_name, db_path=DB_PATH)
            print(f"PHASE2_LEAKAGE={leak_result['leaking_count']}")
            run_all_checks(db_path=DB_PATH, min_race_date=BASELINE_MIN_RACE_DATE)
            print("PHASE2_DQ=PASS")

        feature_rows = _materialize_feature_store(con)
        print(f"FEATURE_STORE_ROWS={feature_rows}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
