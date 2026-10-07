from datetime import date
from pathlib import Path

from pipelines.collect_results import update_results_in_db
from ingestion.db_connect import get_db


def test_updates_duplicate_horse_names_by_event_id(tmp_path):
    db_path = tmp_path / "racing.duckdb"
    con = get_db(str(db_path))
    schema_path = Path(__file__).parents[1] / "sql" / "schema" / "001_create_tables.sql"
    con.execute(schema_path.read_text())
    con.execute(
        """
        INSERT INTO races (
            race_id, source_race_id, course_id, course_name, race_date,
            scheduled_off_utc, event_timestamp_utc, decision_cutoff_utc,
            ingest_timestamp_utc
        ) VALUES
            ('race_1', '261851545', 'newcastle', 'Newcastle', DATE '2026-09-03',
             TIMESTAMPTZ '2026-09-03 15:35:00+00:00', TIMESTAMPTZ '2026-09-03 15:35:00+00:00',
             TIMESTAMPTZ '2026-09-02 21:00:00+00:00', NOW()),
            ('race_2', '261851552', 'newcastle', 'Newcastle', DATE '2026-09-03',
             TIMESTAMPTZ '2026-09-03 16:10:00+00:00', TIMESTAMPTZ '2026-09-03 16:10:00+00:00',
             TIMESTAMPTZ '2026-09-02 21:00:00+00:00', NOW())
        """
    )
    con.execute(
        """
        INSERT INTO runners (
            runner_id, race_id, horse_id, horse_name, event_timestamp_utc,
            decision_cutoff_utc
        ) VALUES
            ('runner_1', 'race_1', 'victory_sound', 'Victory Sound', NOW(), NOW()),
            ('runner_2', 'race_2', 'victory_sound', 'Victory Sound', NOW(), NOW())
        """
    )
    con.execute(
        """
        INSERT INTO results (
            result_id, race_id, runner_id, horse_id, won,
            event_timestamp_utc, decision_cutoff_utc
        ) VALUES
            ('result_1', 'race_1', 'runner_1', 'victory_sound', FALSE, NOW(), NOW()),
            ('result_2', 'race_2', 'runner_2', 'victory_sound', FALSE, NOW(), NOW())
        """
    )
    con.close()

    stats = update_results_in_db(
        [{
            "event_id": "261851545",
            "compact_horse": "victorysound",
            "sp_decimal": 3.45,
            "won": True,
            "finishing_position": 1,
        }],
        date(2026, 9, 3),
        db_path,
    )

    con = get_db(str(db_path))
    rows = con.execute(
        "SELECT result_id, sp_decimal, won, finishing_position FROM results ORDER BY result_id"
    ).fetchall()
    con.close()

    assert stats == {"matched": 1, "updated": 1, "not_found": 0}
    assert rows[0][0] == "result_1"
    assert abs(rows[0][1] - 3.45) < 1e-5
    assert rows[0][2:] == (True, 1)
    assert rows[1][0] == "result_2"
    assert rows[1][1:] == (None, False, None)