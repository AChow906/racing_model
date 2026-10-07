CREATE OR REPLACE TABLE f010 AS
WITH base AS (
    SELECT
        ru.runner_id,
        ru.race_id,
        ru.horse_id,
        ru.official_rating AS current_official_rating,
        ra.decision_cutoff_utc
    FROM runners ru
    JOIN races ra ON ra.race_id = ru.race_id
),
prior_wins AS (
    SELECT
        b.runner_id,
        hh.scheduled_off_utc AS win_off_utc,
        hh.official_rating AS win_official_rating,
        ROW_NUMBER() OVER (
            PARTITION BY b.runner_id
            ORDER BY hh.scheduled_off_utc DESC
        ) AS win_rn
    FROM base b
    JOIN horse_history hh
        ON hh.horse_id = b.horse_id
       AND hh.won = TRUE
       AND hh.scheduled_off_utc < b.decision_cutoff_utc
       AND hh.official_rating IS NOT NULL
),
win_summary AS (
    SELECT
        runner_id,
        MAX(win_official_rating) AS best_win_official_rating,
        MAX(win_official_rating) FILTER (WHERE win_rn = 1) AS last_win_official_rating,
        MAX(win_off_utc) FILTER (WHERE win_rn = 1) AS last_win_off_utc
    FROM prior_wins
    GROUP BY 1
),
post_win_peak AS (
    SELECT
        b.runner_id,
        MAX(hh.official_rating) AS peak_rating_since_last_win
    FROM base b
    JOIN win_summary ws ON ws.runner_id = b.runner_id
    JOIN horse_history hh
        ON hh.horse_id = b.horse_id
       AND hh.scheduled_off_utc > ws.last_win_off_utc
       AND hh.scheduled_off_utc < b.decision_cutoff_utc
       AND hh.official_rating IS NOT NULL
    GROUP BY 1
)
SELECT
    b.runner_id,
    b.race_id,
    ws.last_win_official_rating,
    ws.best_win_official_rating,
    pp.peak_rating_since_last_win,
    CASE
        WHEN b.current_official_rating IS NOT NULL
             AND ws.last_win_official_rating IS NOT NULL
        THEN b.current_official_rating - ws.last_win_official_rating
        ELSE NULL
    END AS rating_vs_last_win_mark,
    CASE
        WHEN b.current_official_rating IS NOT NULL
             AND ws.best_win_official_rating IS NOT NULL
        THEN b.current_official_rating - ws.best_win_official_rating
        ELSE NULL
    END AS rating_vs_best_win_mark,
    CASE
        WHEN pp.peak_rating_since_last_win IS NOT NULL
             AND b.current_official_rating IS NOT NULL
        THEN pp.peak_rating_since_last_win - b.current_official_rating
        ELSE NULL
    END AS rating_drop_from_post_win_peak,
    CASE
        WHEN pp.peak_rating_since_last_win IS NOT NULL
             AND ws.last_win_official_rating IS NOT NULL
        THEN pp.peak_rating_since_last_win - ws.last_win_official_rating
        ELSE NULL
    END AS rating_rise_after_last_win,
    CASE
        WHEN ws.last_win_official_rating IS NOT NULL
        THEN DATE_DIFF('day', ws.last_win_off_utc, b.decision_cutoff_utc)
        ELSE NULL
    END AS days_since_last_win,
    b.decision_cutoff_utc - INTERVAL 1 SECOND AS event_timestamp_utc,
    b.decision_cutoff_utc
FROM base b
LEFT JOIN win_summary ws ON ws.runner_id = b.runner_id
LEFT JOIN post_win_peak pp ON pp.runner_id = b.runner_id;
