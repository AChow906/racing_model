import json
import os

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import brier_score_loss
from src.ingestion.db_connect import get_db
from src.modeling.train_split import (
    load_data,
    race_softmax,
    renormalize,
    top_pick_win_rate,
)

RATING_TRAJECTORY_FEATURES = [
    "last_win_official_rating",
    "best_win_official_rating",
    "peak_rating_since_last_win",
    "rating_vs_last_win_mark",
    "rating_vs_best_win_mark",
    "rating_drop_from_post_win_peak",
    "rating_rise_after_last_win",
    "days_since_last_win",
]

WINDOWS = [
    {"train_end": "2021-01-01", "cal_start": "2021-01-01", "cal_end": "2022-01-01", "test_start": "2022-01-01", "test_end": "2023-01-01", "label": "Test 2022"},
    {"train_end": "2022-01-01", "cal_start": "2022-01-01", "cal_end": "2023-01-01", "test_start": "2023-01-01", "test_end": "2024-01-01", "label": "Test 2023"},
    {"train_end": "2023-01-01", "cal_start": "2023-01-01", "cal_end": "2024-01-01", "test_start": "2024-01-01", "test_end": "2025-01-01", "label": "Test 2024"},
    {"train_end": "2024-01-01", "cal_start": "2024-01-01", "cal_end": "2025-01-01", "test_start": "2025-01-01", "test_end": "2027-01-01", "label": "Test 2025-26"},
]


def run_window(category, window, rating_trajectory_mode):
    X_train, y_train, g_train, df_train = load_data("2015-01-01", window["train_end"], category)
    X_cal, y_cal, g_cal, df_cal = load_data(window["cal_start"], window["cal_end"], category)
    X_test, y_test, g_test, df_test = load_data(window["test_start"], window["test_end"], category)

    if len(X_test) == 0:
        return None

    if rating_trajectory_mode == "none":
        X_train = X_train.drop(columns=RATING_TRAJECTORY_FEATURES, errors="ignore")
        X_cal = X_cal.drop(columns=RATING_TRAJECTORY_FEATURES, errors="ignore")
        X_test = X_test.drop(columns=RATING_TRAJECTORY_FEATURES, errors="ignore")
    elif rating_trajectory_mode == "handicap_only":
        for X, df in [(X_train, df_train), (X_cal, df_cal), (X_test, df_test)]:
            handicap = df["is_handicap"].fillna(False).astype(bool).to_numpy()
            X.loc[~handicap, RATING_TRAJECTORY_FEATURES] = np.nan

    non_numeric = [c for c in X_train.columns
                   if not (pd.api.types.is_numeric_dtype(X_train[c]) or pd.api.types.is_bool_dtype(X_train[c]))]
    for col in non_numeric:
        cats = pd.Index(pd.concat([X_train[col], X_cal[col], X_test[col]]).astype(str).astype("category").cat.categories)
        X_train[col] = pd.Categorical(X_train[col].astype(str), categories=cats).codes
        X_cal[col] = pd.Categorical(X_cal[col].astype(str), categories=cats).codes
        X_test[col] = pd.Categorical(X_test[col].astype(str), categories=cats).codes

    model = lgb.LGBMRanker(
        objective="lambdarank", n_estimators=3000, learning_rate=0.01,
        num_leaves=63, min_child_samples=20, subsample=0.8,
        colsample_bytree=0.8, subsample_freq=1, random_state=42, n_jobs=1,
        deterministic=True, feature_fraction_seed=42, bagging_seed=42,
        data_random_seed=42,
    )
    model.fit(
        X_train, y_train, group=g_train,
        eval_set=[(X_cal, y_cal)], eval_group=[g_cal], eval_at=[1, 3],
        callbacks=[lgb.early_stopping(200, first_metric_only=True), lgb.log_evaluation(500)],
    )
    importance = dict(zip(model.feature_name_, model.feature_importances_))
    rating_importance = {
        name: int(importance.get(name, 0))
        for name in RATING_TRAJECTORY_FEATURES
        if name in importance
    }

    cal_ids = df_cal["race_id"].to_numpy()
    cal_probs = race_softmax(model.predict(X_cal, num_iteration=model.best_iteration_), cal_ids)
    calibrator = IsotonicRegression(out_of_bounds="clip")
    calibrator.fit(cal_probs, y_cal)

    test_ids = df_test["race_id"].to_numpy()
    test_scores = model.predict(X_test, num_iteration=model.best_iteration_)
    test_probs = renormalize(calibrator.transform(race_softmax(test_scores, test_ids)), test_ids)

    tpwr = top_pick_win_rate(test_probs, test_ids, y_test)
    brier = brier_score_loss(y_test, test_probs)

    db = get_db(os.environ.get("RACING_DB", "racing.duckdb"))
    sp_df = db.execute("SELECT runner_id, sp_decimal FROM results WHERE sp_decimal > 1").df()
    db.close()

    a = df_test[["race_id", "runner_id"]].copy()
    a["prob"] = test_probs
    a["target"] = y_test
    a = a.merge(sp_df, on="runner_id", how="left")
    a["implied"] = 1.0 / a["sp_decimal"]
    a["edge"] = a["prob"] - a["implied"]
    a["profit"] = a["target"] * (a["sp_decimal"] - 1) - (1 - a["target"])
    has_sp = a[a["sp_decimal"].notna()]

    results = {}
    for thresh in [0.03, 0.05, 0.10]:
        vb = has_sp[has_sp["edge"] > thresh]
        if len(vb) > 0:
            results[f"edge_{int(thresh*100)}"] = {
                "bets": len(vb),
                "strike": float(vb["target"].mean()),
                "roi": float(vb["profit"].mean()),
                "pnl": float(vb["profit"].sum()),
            }

    return {
        "label": window["label"],
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "test_races": len(g_test),
        "top_pick": float(tpwr),
        "brier": float(brier),
        "best_iter": model.best_iteration_,
        "feature_count": len(X_train.columns),
        "rating_trajectory_mode": rating_trajectory_mode,
        "rating_trajectory_importance": rating_importance,
        "value": results,
    }


def main():
    all_results = {}
    for rating_trajectory_mode in ["none", "all_races", "handicap_only"]:
        variant = {
            "none": "baseline",
            "all_races": "with_rating_trajectory",
            "handicap_only": "handicap_only_rating_trajectory",
        }[rating_trajectory_mode]
        all_results[variant] = {}
        for category in ["flat", "jumps"]:
            print(f"\n{'='*70}", flush=True)
            print(f"  {category.upper()} — {variant.upper()} WALK-FORWARD", flush=True)
            print(f"{'='*70}", flush=True)

            cat_results = []
            for w in WINDOWS:
                print(f"\n  --- {w['label']} (train to {w['train_end']}) ---", flush=True)
                result = run_window(category, w, rating_trajectory_mode)
                if result is None:
                    continue
                cat_results.append(result)
                print(f"  Features: {result['feature_count']} | Test: {result['test_rows']:,} rows, {result['test_races']:,} races", flush=True)
                print(f"  TopPick: {result['top_pick']:.1%} | Brier: {result['brier']:.5f} | Trees: {result['best_iter']}", flush=True)
                for key, v in result["value"].items():
                    print(f"  {key}: {v['bets']:>5} bets, strike={v['strike']:.3f}, ROI={v['roi']:>+7.2%}, P&L=£{v['pnl']:>+7.0f}", flush=True)
            all_results[variant][category] = cat_results

    print(f"\n{'='*70}", flush=True)
    print("  A/B WALK-FORWARD DELTA (VARIANT MINUS BASELINE)", flush=True)
    print(f"{'='*70}", flush=True)
    for category in ["flat", "jumps"]:
        base_rows = {r["label"]: r for r in all_results["baseline"][category]}
        for variant in ["with_rating_trajectory", "handicap_only_rating_trajectory"]:
            new_rows = {r["label"]: r for r in all_results[variant][category]}
            total_base = total_new = 0.0
            total_base_bets = total_new_bets = 0
            for label in base_rows:
                b = base_rows[label]["value"].get("edge_5", {})
                n = new_rows[label]["value"].get("edge_5", {})
                total_base += b.get("pnl", 0.0)
                total_new += n.get("pnl", 0.0)
                total_base_bets += b.get("bets", 0)
                total_new_bets += n.get("bets", 0)
                print(f"{category} {variant} {label}: ROI {b.get('roi', 0):+.2%} -> {n.get('roi', 0):+.2%}; "
                      f"P&L £{b.get('pnl', 0):+.0f} -> £{n.get('pnl', 0):+.0f}; "
                      f"TopPick {base_rows[label]['top_pick']:.1%} -> {new_rows[label]['top_pick']:.1%}", flush=True)
            base_roi = total_base / total_base_bets if total_base_bets else 0
            new_roi = total_new / total_new_bets if total_new_bets else 0
            print(f"{category} {variant} TOTAL: ROI {base_roi:+.2%} -> {new_roi:+.2%}; "
                  f"P&L £{total_base:+.0f} -> £{total_new:+.0f}; "
                  f"Bets {total_base_bets:,} -> {total_new_bets:,}", flush=True)

    # Save
    with open("experiments/walk_forward_results.json", "w") as f:
        json.dump(all_results, f, indent=2)
    print("\nSaved: experiments/walk_forward_results.json", flush=True)


if __name__ == "__main__":
    main()
