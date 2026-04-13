"""
zscores.py — Z-score computation engine.

This module is the computational heart of the app. It takes cleaned
FanGraphs projection DataFrames, computes derived stats, selects the
player pool, and calculates Z-scores for all 12 fantasy categories.

Z-score formula
---------------
  z = (player_value - pool_mean) / pool_std

For rate stats (OBP, ERA, WHIP), the pool mean is weighted by playing
time (PA for OBP; IP for ERA/WHIP) to avoid small-sample bias.

Negative categories (ERA, WHIP): Z-scores are negated so that lower
values produce higher (better) Z-scores.

Shohei Ohtani special handling
-------------------------------
Ohtani is present in both batter and pitcher projections on FanGraphs.
He contributes to all 12 categories. His total_z is the sum of all 6
batting Z-scores and all 6 pitching Z-scores. He appears as one unified
row in the output with is_ohtani = 1.

Output schema
-------------
The returned DataFrame matches the computed_zscores table in database.py.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from lib.config import settings
from lib.utils import stat_helpers, player_matching

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Player pool selection
# ---------------------------------------------------------------------------


def classify_sp_rp(pitchers_df: pd.DataFrame) -> tuple:
    """
    Split pitchers into SP (starters) and RP (relievers) based on their
    games-started-to-total-games ratio.

    Classification rule:
      - If GS >= G * 0.5  →  SP  (at least half their appearances are starts)
      - Otherwise          →  RP

    This is the standard heuristic used by most fantasy baseball tools.
    Edge cases: pitchers with GS == 0 and G == 0 default to RP.

    Parameters
    ----------
    pitchers_df : pd.DataFrame
        Full pitcher projections (must have 'gs' and 'g' columns).

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (sp_df, rp_df) — disjoint subsets of pitchers_df.
    """
    gs = pitchers_df["gs"].fillna(0)
    g = pitchers_df["g"].fillna(0)

    is_sp = gs >= (g * 0.5)
    sp_df = pitchers_df[is_sp].copy()
    rp_df = pitchers_df[~is_sp].copy()

    logger.info(
        f"Pitcher classification: {len(sp_df)} SPs, {len(rp_df)} RPs "
        f"(GS >= G*0.5 rule)"
    )
    return sp_df, rp_df


def build_player_pools(
    batters_df: pd.DataFrame,
    sp_df: pd.DataFrame,
    rp_df: pd.DataFrame,
    num_teams: int,
) -> tuple:
    """
    Select the "relevant player pool" for Z-score mean/std calculations.

    The pool represents the set of players likely to be rostered in this
    league. Pool sizes:
      - Hitters: num_teams * HITTER_POOL_FACTOR (~168 in 12-team)
      - SPs:     num_teams * SP_POOL_FACTOR     (~84 in 12-team)
      - RPs:     num_teams * RP_POOL_FACTOR     (~36 in 12-team)

    SP and RP have separate pools because combining them dramatically
    overvalues relievers. With only 2 RP roster slots per team, replacement-
    level RP is roughly the 24th-best reliever — not the 120th-best pitcher.

    Parameters
    ----------
    batters_df : pd.DataFrame
        Full cleaned batter projections (already has derived stats added).
    sp_df : pd.DataFrame
        Starting pitchers (from classify_sp_rp).
    rp_df : pd.DataFrame
        Relief pitchers (from classify_sp_rp).
    num_teams : int

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        (hitter_pool, sp_pool, rp_pool) — subsets of the input DataFrames.
    """
    hitter_pool_size = num_teams * settings.HITTER_POOL_FACTOR
    sp_pool_size = num_teams * settings.SP_POOL_FACTOR
    rp_pool_size = num_teams * settings.RP_POOL_FACTOR

    # Sort by playing time (proxy for roster-worthiness) and take top N
    hitter_pool = (
        batters_df.sort_values("pa", ascending=False)
        .head(hitter_pool_size)
    )
    sp_pool = (
        sp_df.sort_values("ip", ascending=False)
        .head(sp_pool_size)
    )
    rp_pool = (
        rp_df.sort_values("ip", ascending=False)
        .head(rp_pool_size)
    )

    logger.info(
        f"Player pools: {len(hitter_pool)} hitters, "
        f"{len(sp_pool)} SPs, {len(rp_pool)} RPs "
        f"(num_teams={num_teams})"
    )
    return hitter_pool, sp_pool, rp_pool


# ---------------------------------------------------------------------------
# Z-score calculation helpers
# ---------------------------------------------------------------------------


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    """
    Compute weighted mean. Returns simple mean if all weights are zero.

    Used for rate stats: OBP weighted by PA, ERA/WHIP weighted by IP.
    """
    total_weight = weights.sum()
    if total_weight == 0:
        return float(values.mean())
    return float((values * weights).sum() / total_weight)


def calc_category_zscores(
    player_df: pd.DataFrame,
    pool_df: pd.DataFrame,
    stat_col: str,
    weight_col: str = None,
) -> pd.Series:
    """
    Calculate Z-scores for one stat category across all players.

    The pool (not the full player set) is used to compute the reference
    mean and standard deviation. Z-scores are computed for every player
    in player_df, even those outside the pool.

    Parameters
    ----------
    player_df : pd.DataFrame
        Full player set (all players get a Z-score).
    pool_df : pd.DataFrame
        Subset of player_df representing the relevant player pool.
    stat_col : str
        Column name of the statistic to compute Z-scores for.
    weight_col : str, optional
        Column name of the weighting variable for rate stats.
        None → simple (unweighted) mean for counting stats.

    Returns
    -------
    pd.Series
        Z-scores indexed the same as player_df. NaN where stat is missing.
    """
    pool_vals = pool_df[stat_col].dropna()
    std = float(pool_vals.std())

    if np.isnan(std):
        logger.warning(f"NaN std dev for category '{stat_col}'. Returning 0.")
        return pd.Series(0.0, index=player_df.index)

    # Apply minimum std floor to prevent extreme Z-scores when most players
    # share the same value (e.g., NSV where most pitchers have 0 net saves).
    if std < settings.MIN_Z_STD:
        logger.info(
            f"Std dev for '{stat_col}' is {std:.4f}, below MIN_Z_STD={settings.MIN_Z_STD}. "
            f"Using floor value."
        )
        std = settings.MIN_Z_STD

    if weight_col is not None:
        pool_weights = pool_df.loc[pool_vals.index, weight_col].fillna(0.0)
        mean = _weighted_mean(pool_vals, pool_weights)
    else:
        mean = float(pool_vals.mean())

    z = (player_df[stat_col] - mean) / std
    return z


# ---------------------------------------------------------------------------
# Main Z-score computation pipeline
# ---------------------------------------------------------------------------


def compute_all_zscores(
    batters_df: pd.DataFrame,
    pitchers_df: pd.DataFrame,
    num_teams: int,
    fantrax_lookup: dict = None,
) -> pd.DataFrame:
    """
    Full Z-score computation pipeline.

    Steps:
    1. Add derived stats (TB, NSB, QA3, NSV) to both DataFrames.
    2. Handle Ohtani: remove him from both, compute combined Z-scores separately.
    3. Classify pitchers into SP and RP (based on GS/G ratio).
    4. Build separate player pools: hitters, SPs, RPs.
    5. Compute per-category Z-scores:
       - Batters against hitter pool
       - SPs against SP pool (top ~84 starters in 12-team)
       - RPs against RP pool (top ~36 relievers in 12-team)
    6. Ohtani uses hitter pool (batting) + SP pool (pitching).
    7. Merge into one unified DataFrame.
    8. Optionally link to Fantrax player IDs via fantrax_lookup.
    9. Compute position-relative Z-scores.

    Parameters
    ----------
    batters_df : pd.DataFrame
        Cleaned batter projections (from fangraphs_data.py).
    pitchers_df : pd.DataFrame
        Cleaned pitcher projections (from fangraphs_data.py).
    num_teams : int
        Number of teams in the league (from Fantrax API).
    fantrax_lookup : dict, optional
        Built by player_matching.build_fantrax_lookup(). If None, Fantrax
        IDs will not be linked (fantrax_id column will be None).

    Returns
    -------
    pd.DataFrame
        Unified player Z-score table matching the computed_zscores schema,
        sorted by total_z descending.
    """
    now_ts = datetime.now(timezone.utc).isoformat()

    # --- Step 1: Add derived stats ---
    batters_df = stat_helpers.add_derived_batting_stats(batters_df)
    pitchers_df = stat_helpers.add_derived_pitching_stats(pitchers_df)

    # --- Step 2: Identify and separate Ohtani ---
    ohtani_bat, ohtani_pit = player_matching.identify_ohtani(batters_df, pitchers_df)

    # Remove Ohtani from the main pools (he'll be handled separately)
    ohtani_norm = player_matching.normalize_name("Shohei Ohtani")
    batters_no_ohtani = batters_df[
        batters_df["name"].apply(player_matching.normalize_name) != ohtani_norm
    ].copy()
    pitchers_no_ohtani = pitchers_df[
        pitchers_df["name"].apply(player_matching.normalize_name) != ohtani_norm
    ].copy()

    # --- Step 2b: Reclassify misassigned pitchers before pool selection ---
    # Pitchers with >85 projected IP but classified as RP are really starters.
    # Pitchers with >1 projected NSV but classified as SP are really relievers.
    # Adjust their GS/G ratio so classify_sp_rp places them correctly.
    _gs = pitchers_no_ohtani["gs"].fillna(0)
    _g = pitchers_no_ohtani["g"].fillna(0)
    _ip = pitchers_no_ohtani["ip"].fillna(0)
    _nsv = pitchers_no_ohtani.get("nsv", pd.Series(0, index=pitchers_no_ohtani.index)).fillna(0)

    # RP with >85 IP → force SP classification (set GS = G so GS >= G*0.5)
    force_sp_mask = (_gs < _g * 0.5) & (_ip > 85)
    if force_sp_mask.any():
        names = pitchers_no_ohtani.loc[force_sp_mask, "name"].tolist()
        logger.info(f"Reclassifying {len(names)} pitchers from RP→SP (IP>85): {names}")
        pitchers_no_ohtani.loc[force_sp_mask, "gs"] = pitchers_no_ohtani.loc[force_sp_mask, "g"]

    # SP with >1 NSV → force RP classification (set GS = 0 so GS < G*0.5)
    force_rp_mask = (_gs >= _g * 0.5) & (_nsv > 1)
    if force_rp_mask.any():
        names = pitchers_no_ohtani.loc[force_rp_mask, "name"].tolist()
        logger.info(f"Reclassifying {len(names)} pitchers from SP→RP (NSV>1): {names}")
        pitchers_no_ohtani.loc[force_rp_mask, "gs"] = 0

    # --- Step 3: Classify pitchers into SP and RP ---
    sp_df, rp_df = classify_sp_rp(pitchers_no_ohtani)

    # --- Step 4: Build position-specific player pools ---
    hitter_pool, sp_pool, rp_pool = build_player_pools(
        batters_no_ohtani, sp_df, rp_df, num_teams
    )

    # --- Step 5a: Batting Z-scores ---
    bat_z_df = _compute_batting_zscores(batters_no_ohtani, hitter_pool)

    # --- Step 5b: SP Z-scores (against SP pool) ---
    sp_z_df = _compute_pitching_zscores(sp_df, sp_pool)

    # --- Step 5c: RP Z-scores (against RP pool, then scaled down) ---
    rp_z_df = _compute_pitching_zscores(rp_df, rp_pool)

    # Scale RP Z-scores to reflect that you only roster 2 RPs (vs 5 SPs).
    # Without this, the best closer's pit_z rivals the best SP's pit_z,
    # which dramatically overvalues relievers in overall rankings.
    scale = settings.RP_Z_SCALE_FACTOR
    pit_z_cols = ["ip_z", "whip_z", "k_z", "era_z", "qa3_z", "nsv_z"]
    for col in pit_z_cols:
        rp_z_df[col] = rp_z_df[col] * scale
    rp_z_df["pit_z"] = rp_z_df[pit_z_cols].sum(axis=1)
    logger.info(
        f"RP Z-scores scaled by {scale:.2f}. "
        f"Top RP pit_z after scaling: {rp_z_df['pit_z'].max():.2f}"
    )

    # --- Step 6: Ohtani combined Z-scores ---
    # Ohtani is a SP/DH hybrid — use the SP pool for his pitching Z-scores
    ohtani_row = None
    if ohtani_bat is not None and ohtani_pit is not None:
        ohtani_row = _compute_ohtani_zscores(
            ohtani_bat, ohtani_pit, hitter_pool, sp_pool
        )

    # --- Step 7: Build unified output ---
    batter_rows = _build_batter_output_rows(bat_z_df, batters_no_ohtani, now_ts)
    sp_rows = _build_pitcher_output_rows(sp_z_df, sp_df, now_ts)
    rp_rows = _build_pitcher_output_rows(rp_z_df, rp_df, now_ts)

    all_rows = batter_rows + sp_rows + rp_rows
    if ohtani_row is not None:
        all_rows.append(ohtani_row)

    result_df = pd.DataFrame(all_rows)
    result_df["computed_at"] = now_ts

    # --- Step 8: Link Fantrax IDs ---
    if fantrax_lookup:
        result_df = _link_fantrax_ids(result_df, fantrax_lookup)
    else:
        result_df["fantrax_id"] = None

    # --- Step 9: Compute position-relative Z-scores ---
    result_df = _compute_position_relative_zscores(result_df, num_teams)

    # --- Step 10: Sort by total_z descending ---
    result_df = result_df.sort_values("total_z", ascending=False).reset_index(drop=True)

    logger.info(
        f"Z-score computation complete: {len(result_df)} players. "
        f"Top player: {result_df.iloc[0]['display_name']} "
        f"(total_z={result_df.iloc[0]['total_z']:.2f})"
    )

    return result_df


# ---------------------------------------------------------------------------
# Category-specific computation helpers
# ---------------------------------------------------------------------------


def _compute_batting_zscores(
    batters_df: pd.DataFrame,
    hitter_pool: pd.DataFrame,
) -> pd.DataFrame:
    """Compute Z-scores for all 6 batting categories. Returns a DataFrame
    with columns: fg_playerid, r_z, hr_z, rbi_z, tb_z, obp_z, nsb_z, hit_z"""

    df = batters_df.copy()

    df["r_z"]   = calc_category_zscores(df, hitter_pool, "r")
    df["hr_z"]  = calc_category_zscores(df, hitter_pool, "hr")
    df["rbi_z"] = calc_category_zscores(df, hitter_pool, "rbi")
    df["tb_z"]  = calc_category_zscores(df, hitter_pool, "tb")
    # OBP is a rate stat — weight by PA to compute the pool mean
    df["obp_z"] = calc_category_zscores(df, hitter_pool, "obp", weight_col="pa")
    df["nsb_z"] = calc_category_zscores(df, hitter_pool, "nsb")

    bat_z_cols = ["r_z", "hr_z", "rbi_z", "tb_z", "obp_z", "nsb_z"]
    df["hit_z"] = df[bat_z_cols].sum(axis=1)

    return df


def _compute_pitching_zscores(
    pitchers_df: pd.DataFrame,
    pitcher_pool: pd.DataFrame,
) -> pd.DataFrame:
    """Compute Z-scores for all 6 pitching categories. Returns a DataFrame
    with columns: fg_playerid, ip_z, whip_z, k_z, era_z, qa3_z, nsv_z, pit_z"""

    df = pitchers_df.copy()

    df["ip_z"]   = calc_category_zscores(df, pitcher_pool, "ip")
    df["k_z"]    = calc_category_zscores(df, pitcher_pool, "k")
    df["qa3_z"]  = calc_category_zscores(df, pitcher_pool, "qa3")
    df["nsv_z"]  = calc_category_zscores(df, pitcher_pool, "nsv")
    # ERA and WHIP: rate stats weighted by IP; then negated (lower is better)
    df["whip_z"] = -calc_category_zscores(df, pitcher_pool, "whip", weight_col="ip")
    df["era_z"]  = -calc_category_zscores(df, pitcher_pool, "era",  weight_col="ip")

    pit_z_cols = ["ip_z", "whip_z", "k_z", "era_z", "qa3_z", "nsv_z"]
    df["pit_z"] = df[pit_z_cols].sum(axis=1)

    return df


def _compute_ohtani_zscores(
    ohtani_bat: pd.Series,
    ohtani_pit: pd.Series,
    hitter_pool: pd.DataFrame,
    pitcher_pool: pd.DataFrame,
) -> dict:
    """
    Compute Ohtani's Z-scores across all 12 categories.

    Ohtani is the only player who contributes to both batting and pitching
    categories. His total_z is the sum of all 12 individual category Z-scores.

    Returns a dict row matching the output schema.
    """
    # Wrap single rows in single-row DataFrames for compatibility with helpers
    bat_df = pd.DataFrame([ohtani_bat])
    bat_df = stat_helpers.add_derived_batting_stats(bat_df)
    bat_z = _compute_batting_zscores(bat_df, hitter_pool)

    pit_df = pd.DataFrame([ohtani_pit])
    pit_df = stat_helpers.add_derived_pitching_stats(pit_df)
    pit_z = _compute_pitching_zscores(pit_df, pitcher_pool)

    # Determine player key — use batter fg_playerid as primary
    bat_id = str(ohtani_bat.get("fg_playerid", "ohtani-bat"))
    pit_id = str(ohtani_pit.get("fg_playerid", "ohtani-pit"))

    row = {
        "player_key":    f"ohtani-{bat_id}",
        "fg_batter_id":  bat_id,
        "fg_pitcher_id": pit_id,
        "display_name":  str(ohtani_bat.get("name", "Shohei Ohtani")),
        "mlb_team":      str(ohtani_bat.get("team", "")),
        "positions":     "DH,SP",  # Ohtani is listed as DH (hitter) and SP (pitcher)
        "is_ohtani":     1,
        # Raw projections
        "proj_pa":       float(bat_df["pa"].iloc[0]) if "pa" in bat_df.columns else None,
        "proj_g":        float(bat_df["g"].iloc[0]) if "g" in bat_df.columns else None,
        "proj_r":        float(bat_df["r"].iloc[0]),
        "proj_hr":       float(bat_df["hr"].iloc[0]),
        "proj_rbi":      float(bat_df["rbi"].iloc[0]),
        "proj_tb":       float(bat_df["tb"].iloc[0]),
        "proj_obp":      float(bat_df["obp"].iloc[0]) if not pd.isna(bat_df["obp"].iloc[0]) else None,
        "proj_nsb":      float(bat_df["nsb"].iloc[0]),
        # Raw projections (pitching)
        "proj_ip":       float(pit_df["ip"].iloc[0]),
        "proj_whip":     float(pit_df["whip"].iloc[0]),
        "proj_k":        float(pit_df["k"].iloc[0]),
        "proj_era":      float(pit_df["era"].iloc[0]),
        "proj_qa3":      float(pit_df["qa3"].iloc[0]),
        "proj_nsv":      float(pit_df["nsv"].iloc[0]),
        # Z-scores
        "r_z":    float(bat_z["r_z"].iloc[0]),
        "hr_z":   float(bat_z["hr_z"].iloc[0]),
        "rbi_z":  float(bat_z["rbi_z"].iloc[0]),
        "tb_z":   float(bat_z["tb_z"].iloc[0]),
        "obp_z":  float(bat_z["obp_z"].iloc[0]),
        "nsb_z":  float(bat_z["nsb_z"].iloc[0]),
        "ip_z":   float(pit_z["ip_z"].iloc[0]),
        "whip_z": float(pit_z["whip_z"].iloc[0]),
        "k_z":    float(pit_z["k_z"].iloc[0]),
        "era_z":  float(pit_z["era_z"].iloc[0]),
        "qa3_z":  float(pit_z["qa3_z"].iloc[0]),
        "nsv_z":  float(pit_z["nsv_z"].iloc[0]),
    }

    row["hit_z"]   = sum(row[c] for c in ["r_z", "hr_z", "rbi_z", "tb_z", "obp_z", "nsb_z"])
    row["pit_z"]   = sum(row[c] for c in ["ip_z", "whip_z", "k_z", "era_z", "qa3_z", "nsv_z"])
    row["total_z"] = row["hit_z"] + row["pit_z"]

    return row


# ---------------------------------------------------------------------------
# Output row builders
# ---------------------------------------------------------------------------


def _build_batter_output_rows(
    bat_z_df: pd.DataFrame,
    batters_df: pd.DataFrame,
    now_ts: str,
) -> list:
    """
    Build the list of dict rows for all non-Ohtani batters.
    """
    rows = []
    for idx, row in bat_z_df.iterrows():
        orig = batters_df.loc[idx]
        player_id = str(orig.get("fg_playerid", idx))
        r = {
            "player_key":    f"bat-{player_id}",
            "fg_batter_id":  player_id,
            "fg_pitcher_id": None,
            "display_name":  str(orig.get("name", "")),
            "mlb_team":      str(orig.get("team", "")),
            "positions":     "",   # filled later from Fantrax roster data
            "is_ohtani":     0,
            # Raw projections
            "proj_pa":       _safe_float(orig.get("pa")),
            "proj_g":        _safe_float(orig.get("g")),
            "proj_r":        _safe_float(orig.get("r")),
            "proj_hr":       _safe_float(orig.get("hr")),
            "proj_rbi":      _safe_float(orig.get("rbi")),
            "proj_tb":       _safe_float(orig.get("tb")),
            "proj_obp":      _safe_float(orig.get("obp")),
            "proj_nsb":      _safe_float(orig.get("nsb")),
            "proj_ip":       None,
            "proj_whip":     None,
            "proj_k":        None,
            "proj_era":      None,
            "proj_qa3":      None,
            "proj_nsv":      None,
            # Batting Z-scores
            "r_z":    _safe_float(row.get("r_z")),
            "hr_z":   _safe_float(row.get("hr_z")),
            "rbi_z":  _safe_float(row.get("rbi_z")),
            "tb_z":   _safe_float(row.get("tb_z")),
            "obp_z":  _safe_float(row.get("obp_z")),
            "nsb_z":  _safe_float(row.get("nsb_z")),
            # Pitching Z-scores: 0 for pure batters
            "ip_z":   0.0,
            "whip_z": 0.0,
            "k_z":    0.0,
            "era_z":  0.0,
            "qa3_z":  0.0,
            "nsv_z":  0.0,
            # Aggregates
            "hit_z":   _safe_float(row.get("hit_z")),
            "pit_z":   0.0,
            "total_z": _safe_float(row.get("hit_z")),
        }
        rows.append(r)
    return rows


def _build_pitcher_output_rows(
    pit_z_df: pd.DataFrame,
    pitchers_df: pd.DataFrame,
    now_ts: str,
) -> list:
    """
    Build the list of dict rows for all non-Ohtani pitchers.
    """
    rows = []
    for idx, row in pit_z_df.iterrows():
        orig = pitchers_df.loc[idx]
        player_id = str(orig.get("fg_playerid", idx))
        r = {
            "player_key":    f"pit-{player_id}",
            "fg_batter_id":  None,
            "fg_pitcher_id": player_id,
            "display_name":  str(orig.get("name", "")),
            "mlb_team":      str(orig.get("team", "")),
            "positions":     "",   # filled later from Fantrax roster data
            "is_ohtani":     0,
            # Batting projections: None for pure pitchers
            "proj_pa":       None,
            "proj_g":        _safe_float(orig.get("g")),
            "proj_r":        None,
            "proj_hr":       None,
            "proj_rbi":      None,
            "proj_tb":       None,
            "proj_obp":      None,
            "proj_nsb":      None,
            # Pitching projections
            "proj_ip":       _safe_float(orig.get("ip")),
            "proj_whip":     _safe_float(orig.get("whip")),
            "proj_k":        _safe_float(orig.get("k")),
            "proj_era":      _safe_float(orig.get("era")),
            "proj_qa3":      _safe_float(orig.get("qa3")),
            "proj_nsv":      _safe_float(orig.get("nsv")),
            # Batting Z-scores: 0 for pure pitchers
            "r_z":   0.0,
            "hr_z":  0.0,
            "rbi_z": 0.0,
            "tb_z":  0.0,
            "obp_z": 0.0,
            "nsb_z": 0.0,
            # Pitching Z-scores
            "ip_z":   _safe_float(row.get("ip_z")),
            "whip_z": _safe_float(row.get("whip_z")),
            "k_z":    _safe_float(row.get("k_z")),
            "era_z":  _safe_float(row.get("era_z")),
            "qa3_z":  _safe_float(row.get("qa3_z")),
            "nsv_z":  _safe_float(row.get("nsv_z")),
            # Aggregates
            "hit_z":   0.0,
            "pit_z":   _safe_float(row.get("pit_z")),
            "total_z": _safe_float(row.get("pit_z")),
        }
        rows.append(r)
    return rows


# ---------------------------------------------------------------------------
# Fantrax ID linking
# ---------------------------------------------------------------------------


def _link_fantrax_ids(result_df: pd.DataFrame, fantrax_lookup: dict) -> pd.DataFrame:
    """
    Add fantrax_id and positions columns by matching display_name + mlb_team
    to Fantrax roster.

    The fantrax_lookup dict maps lookup keys to dicts containing
    'player_id' and 'positions'. Positions from Fantrax are used to populate
    multi-position eligibility (e.g., "SS,2B").

    Uses player_matching.match_fg_to_fantrax() internally.
    """
    matched = player_matching.match_fg_to_fantrax(
        result_df[["display_name", "mlb_team", "player_key"]].rename(
            columns={"display_name": "name", "mlb_team": "team", "player_key": "fg_playerid"}
        ),
        fantrax_lookup,
    )
    result_df["fantrax_id"] = matched["fantrax_id"].values

    # Copy Fantrax positions (multi-eligibility) into the Z-scores table.
    # matched may also carry a 'fantrax_positions' column from the lookup.
    if "fantrax_positions" in matched.columns:
        result_df["positions"] = matched["fantrax_positions"].values
    return result_df


# ---------------------------------------------------------------------------
# Position eligibility filtering (used by UI pages)
# ---------------------------------------------------------------------------


def get_position_eligible_players(
    zscores_df: pd.DataFrame,
    position: str,
) -> pd.DataFrame:
    """
    Filter the Z-score table to players eligible at the given position.

    A player appears in a position tab if that position is listed in their
    'positions' column (comma-separated string). The player's full Z-score
    is preserved unchanged — it is not split or adjusted by position.

    For the 'All' tab, the full DataFrame is returned unfiltered.

    Parameters
    ----------
    zscores_df : pd.DataFrame
        Full computed_zscores table (already has 'rank' column added by
        database.read_zscores()).
    position : str
        One of: "All", "C", "1B", "2B", "3B", "SS", "OF", "SP", "RP".

    Returns
    -------
    pd.DataFrame
        Filtered and re-numbered DataFrame (rank resets within position).
    """
    if position == "All":
        return zscores_df.copy()

    # Match any player whose positions column contains the target position
    # as a whole word (e.g., "OF" should not match "COF" or similar)
    mask = zscores_df["positions"].str.contains(
        rf"\b{position}\b", regex=True, na=False, case=False
    )
    filtered = zscores_df[mask].copy()
    filtered = filtered.reset_index(drop=True)
    # Add position-specific rank (1-based within this position)
    filtered.insert(0, "pos_rank", range(1, len(filtered) + 1))
    return filtered


# ---------------------------------------------------------------------------
# Position-relative Z-scores
# ---------------------------------------------------------------------------


def _compute_position_relative_zscores(
    result_df: pd.DataFrame,
    num_teams: int,
) -> pd.DataFrame:
    """
    Compute position-relative Z-scores for each player at each eligible position.

    A position-relative Z-score answers: "How good is this player compared to
    other players eligible at the same position?" This is different from the
    overall total_z, which compares against the full hitter or pitcher pool.

    For each position (C, 1B, 2B, 3B, SS, OF, SP, RP), we:
      1. Find all players eligible at that position.
      2. Take the top N by total_z as the "position pool" (N = num_teams * slots).
      3. Compute mean and std of total_z within that pool.
      4. For every eligible player: pos_z = (total_z - pool_mean) / pool_std.

    The result is stored as a JSON string in the 'pos_z_map' column.
    Example: '{"SS": 2.31, "2B": 1.05}'

    Parameters
    ----------
    result_df : pd.DataFrame
        The unified Z-score table (must have 'positions' and 'total_z').
    num_teams : int
        Number of teams in the league.

    Returns
    -------
    pd.DataFrame
        result_df with 'pos_z_map' column added.
    """
    # How many players per position constitute the "pool" for Z calculation.
    # Roughly: num_teams * number of roster slots for that position.
    # For positions with 1 slot (C, 1B, 2B, 3B, SS): pool = num_teams * 1.5
    # For OF (3 slots): pool = num_teams * 3.5
    # For SP (5 slots): pool = num_teams * 5.5
    # For RP (2 slots): pool = num_teams * 2.5
    # The extra 0.5 accounts for bench depth at that position.
    _POS_POOL_MULTIPLIERS = {
        "C": 1.5, "1B": 1.5, "2B": 1.5, "3B": 1.5, "SS": 1.5,
        "OF": 3.5, "SP": 5.5, "RP": 2.5,
    }

    # Pre-compute position eligibility masks and pool stats
    pos_stats = {}  # {position: (mean, std)}

    for pos, mult in _POS_POOL_MULTIPLIERS.items():
        pool_size = int(num_teams * mult)

        # Find all players eligible at this position
        mask = result_df["positions"].str.contains(
            rf"\b{pos}\b", regex=True, na=False, case=False
        )
        eligible = result_df.loc[mask].copy()

        if eligible.empty:
            pos_stats[pos] = (0.0, settings.MIN_Z_STD)
            continue

        # Take the top N by total_z as the position pool
        pool = eligible.nlargest(min(pool_size, len(eligible)), "total_z")

        pool_mean = float(pool["total_z"].mean())
        pool_std = float(pool["total_z"].std())

        # Apply minimum std floor (same as category Z-scores)
        if np.isnan(pool_std) or pool_std < settings.MIN_Z_STD:
            pool_std = settings.MIN_Z_STD

        pos_stats[pos] = (pool_mean, pool_std)

    logger.info(
        "Position pool stats: "
        + ", ".join(f"{p}: mean={m:.2f} std={s:.2f}" for p, (m, s) in pos_stats.items())
    )

    # Compute pos_z_map for each player
    pos_z_maps = []
    for _, row in result_df.iterrows():
        positions_str = row.get("positions", "")
        total_z = row.get("total_z", 0.0)

        if pd.isna(positions_str) or not positions_str:
            pos_z_maps.append("{}")
            continue

        # Parse the player's eligible positions
        player_positions = [p.strip() for p in positions_str.split(",") if p.strip()]

        pos_z_dict = {}
        for pos in player_positions:
            if pos in pos_stats:
                mean, std = pos_stats[pos]
                pos_z = (total_z - mean) / std
                pos_z_dict[pos] = round(pos_z, 2)

        pos_z_maps.append(json.dumps(pos_z_dict))

    result_df["pos_z_map"] = pos_z_maps
    return result_df


def get_display_z(
    pos_z_map_str: str,
    roster_slot: str,
    positions: str,
    total_z: float,
) -> tuple:
    """
    Determine which Z-score to display for a player based on roster context.

    Rules:
      - Active lineup (roster_slot is a real position like SS, SP, etc.):
        show Z at that rostered position.
      - Bench / unrostered / free agent:
        show Z at their best (highest) position.
      - If pos_z_map is empty or has no data, fall back to total_z.

    Parameters
    ----------
    pos_z_map_str : str
        JSON string from the pos_z_map column.
    roster_slot : str
        The player's current roster slot (SS, SP, BN, etc.) or empty for FA.
    positions : str
        Comma-separated eligible positions.
    total_z : float
        The overall total Z-score (fallback).

    Returns
    -------
    tuple[float, str, bool]
        (display_z_value, display_position, is_multi_eligible)
        - display_z_value: the Z-score to show
        - display_position: which position the Z is for
        - is_multi_eligible: True if the player is eligible at 2+ positions
    """
    try:
        pos_z_map = json.loads(pos_z_map_str) if pos_z_map_str else {}
    except (json.JSONDecodeError, TypeError):
        pos_z_map = {}

    # Parse eligible positions
    pos_list = [p.strip() for p in (positions or "").split(",") if p.strip()]
    is_multi = len(pos_list) > 1

    if not pos_z_map:
        best_pos = pos_list[0] if pos_list else ""
        return total_z, best_pos, is_multi

    # Bench-like slots that aren't real positions
    bench_slots = {"BN", "Bench", "IL", "IR", "Minors", "Reserve", ""}

    if roster_slot and roster_slot not in bench_slots and roster_slot in pos_z_map:
        # Active player at a specific position: use that position's Z
        return pos_z_map[roster_slot], roster_slot, is_multi

    # Bench / FA / unrostered: use best position Z
    if pos_z_map:
        best_pos = max(pos_z_map, key=pos_z_map.get)
        return pos_z_map[best_pos], best_pos, is_multi

    best_pos = pos_list[0] if pos_list else ""
    return total_z, best_pos, is_multi


def format_pos_z_tooltip(pos_z_map_str: str) -> str:
    """
    Format the position Z-score map into a readable string for display.

    Example output: "SS: 2.31 | 2B: 1.05"

    Parameters
    ----------
    pos_z_map_str : str
        JSON string from the pos_z_map column.

    Returns
    -------
    str
        Human-readable position Z-score breakdown, or empty string.
    """
    try:
        pos_z_map = json.loads(pos_z_map_str) if pos_z_map_str else {}
    except (json.JSONDecodeError, TypeError):
        return ""

    if not pos_z_map:
        return ""

    # Sort by Z-score descending (best position first)
    sorted_items = sorted(pos_z_map.items(), key=lambda x: x[1], reverse=True)
    return " | ".join(f"{pos}: {z:.2f}" for pos, z in sorted_items)


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _safe_float(val) -> float | None:
    """Convert a value to float, returning None if conversion fails."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else f
    except (ValueError, TypeError):
        return None
