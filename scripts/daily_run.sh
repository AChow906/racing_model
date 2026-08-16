#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

source .env 2>/dev/null || true

VENV_PYTHON="$PROJECT_DIR/.venv/bin/python"
if [ ! -f "$VENV_PYTHON" ]; then
    VENV_PYTHON="python"
fi

YESTERDAY=$(date -v-1d '+%Y-%m-%d')
YESTERDAY_SLASH=$(echo "$YESTERDAY" | tr '-' '/')
YESTERDAY_YEAR=${YESTERDAY%%-*}
YESTERDAY_MONTH=$(echo "$YESTERDAY" | cut -d- -f2 | sed 's/^0//')
RPSCRAPE_DIR="$PROJECT_DIR/data/raw/rpscrape_repo/scripts"
RPSCRAPE_PYTHON=python

# Days back to re-scrape so late-published RPR gets captured. Racing Post assigns RPR
# 1-3 days after a race, so the most recent days always scrape with partial RPR;
# re-scraping D-2..D-4 each run backfills them once RP finalises the ratings.
RPR_RESCRAPE_LAG_DAYS="2 3 4"
CACHE_PROGRESS_DIR="$PROJECT_DIR/data/raw/rpscrape_repo/.cache/progress"

# Scrape one day (GB + IRE) fresh. rpscrape keeps a .progress checkpoint per day and
# otherwise "resumes after" the last race scraped — a re-run would fetch nothing and
# write an empty CSV. Clearing the checkpoint first forces a full re-scrape so newly
# published RPR actually lands.
rescrape_day() {
    local iso="$1"
    local slash under
    slash=$(echo "$iso" | tr '-' '/')
    under=$(echo "$iso" | tr '-' '_')
    find "$CACHE_PROGRESS_DIR" -name "${under}.progress" -delete 2>/dev/null || true
    (cd "$RPSCRAPE_DIR" && $RPSCRAPE_PYTHON rpscrape.py -d "$slash" -r gb)  || echo "  WARNING: rpscrape GB failed for $iso"
    sleep 6
    (cd "$RPSCRAPE_DIR" && $RPSCRAPE_PYTHON rpscrape.py -d "$slash" -r ire) || echo "  WARNING: rpscrape IRE failed for $iso"
    sleep 6
}

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
echo "=========================================="
echo "  Daily Racing Pipeline — $TIMESTAMP"
echo "=========================================="

# 1. Collect yesterday's results (SP + won status)
echo ""
echo "[1/9] Collecting yesterday's results..."
$VENV_PYTHON -m src.pipelines.collect_results --date yesterday || {
    echo "  WARNING: Results collection failed (CSV may not be available yet)"
}

# 2. Scrape yesterday, then re-scrape recent days so late-published RPR gets backfilled.
echo ""
echo "[2/9] Scraping Racing Post results (yesterday + RPR catch-up)..."
rescrape_day "$YESTERDAY"
for lag in $RPR_RESCRAPE_LAG_DAYS; do
    catchup_date=$(date -v-"${lag}"d '+%Y-%m-%d')
    echo "  RPR catch-up: re-scraping $catchup_date..."
    rescrape_day "$catchup_date"
done

# 3. Ingest yesterday's SP CSV to populate horse_history
echo ""
echo "[3/9] Ingesting SP history for horse_history..."
$VENV_PYTHON -m src.ingestion.betfair_historical \
    --use-sp-history --sp-include pricesukwin,pricesirewin \
    --start-year "$YESTERDAY_YEAR" --start-month "$YESTERDAY_MONTH" \
    --end-year "$YESTERDAY_YEAR" --end-month "$YESTERDAY_MONTH" || {
    echo "  WARNING: SP history ingestion failed"
}

# 4. Enrich runners and horse_history with rpscrape data
echo ""
echo "[4/9] Enriching with rpscrape data..."
$VENV_PYTHON -m src.ingestion.rpscrape_enrich \
    --input-glob "data/raw/rpscrape_repo/data/region/*/all/*.csv" || {
    echo "  WARNING: rpscrape enrichment failed"
}

# 5. Backfill horse_history distance/going from enriched races
echo ""
echo "[5/9] Backfilling horse_history from races..."
$VENV_PYTHON -c "
import sys; sys.path.insert(0, 'src')
from ingestion.db_connect import get_db
con = get_db('racing.duckdb')
con.execute('''
    UPDATE horse_history
    SET distance_furlongs = COALESCE(r.distance_furlongs, horse_history.distance_furlongs),
        going_code = COALESCE(r.going_code, horse_history.going_code),
        event_timestamp_utc = r.scheduled_off_utc,
        decision_cutoff_utc = r.decision_cutoff_utc
    FROM races r
    WHERE horse_history.race_id = r.race_id
      AND (horse_history.distance_furlongs IS NULL
           OR horse_history.event_timestamp_utc != r.scheduled_off_utc)
''')
con.close()
" || {
    echo "  WARNING: horse_history backfill failed"
}

# 6. Update P&L tracker
echo ""
echo "[6/9] Updating P&L tracker..."
$VENV_PYTHON -m src.pipelines.track_pnl || {
    echo "  WARNING: P&L update failed (no bets logged yet?)"
}

# 7. Scrape today's racecards from Racing Post
echo ""
echo "[7/9] Scraping today's racecards..."
$RPSCRAPE_PYTHON "$PROJECT_DIR/scripts/scrape_racecards.py" --date today || {
    echo "  WARNING: Racecard scrape failed (scoring will proceed without enrichment)"
}

# 8. Fetch today's cards, enrich with racecards, and score
echo ""
echo "[8/9] Scoring today's races..."
$VENV_PYTHON -m src.pipelines.daily_predictions --date today

# 9. AI analysis + publish to Discord (picks + yesterday's results)
echo ""
echo "[9/9] Publishing to Discord..."
$VENV_PYTHON -m src.pipelines.publish_discord --date today || {
    echo "  WARNING: Discord publish failed (check bot token / channel IDs / LLM key)"
}

echo ""
echo "=========================================="
echo "  Pipeline complete — $(date '+%H:%M:%S')"
echo "  Check logs/daily_bets.csv for today's bets"
echo "  Check logs/pnl_tracker.csv for P&L"
echo "  FADE picks (if any): run scripts review — python -m src.pipelines.review_pending"
echo "=========================================="
