"""
generate-rankings.py — Fetch FanGraphs projections, compute Z-scores, output JSON.

This script runs daily (via GitHub Actions or manually) and produces a JSON file
in public/data/ that the Player Rankings page reads client-side.

Usage:
    cd Website
    py scripts/generate-rankings.py

Environment variables (optional, defaults are hardcoded in settings.py):
    FANTRAX_LEAGUE_ID       — Fantrax league ID
    FANTRAX_USER_SECRET_ID  — Fantrax user secret ID
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

# Add the scripts directory to Python path so `lib` is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

from lib.config import settings
from lib.core.fangraphs_data import fetch_projection_system
from lib.core.fantrax_api import (
    get_league_info,
    get_team_rosters,
    parse_league_info,
    parse_team_rosters,
    build_player_name_lookup,
)
from lib.core.zscores import compute_all_zscores
from lib.utils.player_matching import build_fantrax_lookup

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("generate-rankings")

# Output directory (relative to project root)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "public", "data")


def fetch_fantrax_data() -> tuple:
    """
    Fetch league info and rosters from the Fantrax API.

    Returns
    -------
    tuple of (fantrax_lookup, rosters_df, num_teams)
        - fantrax_lookup: dict for matching FanGraphs players to Fantrax IDs
        - rosters_df: DataFrame of all rostered players with team ownership
        - num_teams: int, number of teams in the league
    """
    logger.info("Fetching Fantrax league data...")

    # Get player name directory (maps short IDs to names)
    player_lookup = build_player_name_lookup()
    logger.info(f"Player name lookup: {len(player_lookup)} entries")

    # Get league info (team names/IDs)
    raw_info = get_league_info()
    teams_df, num_teams = parse_league_info(raw_info)
    logger.info(f"League has {num_teams} teams")

    # Get rosters (who owns which player)
    raw_rosters = get_team_rosters()
    rosters_df = parse_team_rosters(raw_rosters, player_lookup)
    logger.info(f"Loaded {len(rosters_df)} roster entries")

    # Build the lookup dict that the Z-score engine uses to link players
    fantrax_lookup = build_fantrax_lookup(rosters_df)

    # Join team names onto rosters (rosters_df has team_id, teams_df has team_id + team_name)
    if "team_name" not in rosters_df.columns and not teams_df.empty:
        rosters_df = rosters_df.merge(
            teams_df[["team_id", "team_name"]],
            on="team_id",
            how="left",
        )

    return fantrax_lookup, rosters_df, num_teams


def build_ownership_map(rosters_df: pd.DataFrame) -> dict:
    """
    Build two dicts for roster ownership lookup:
    1. By Fantrax player_id -> team_name (most reliable)
    2. By normalized player_name -> team_name (fallback for unmatched IDs)

    Parameters
    ----------
    rosters_df : pd.DataFrame
        Roster data from Fantrax API with 'player_id', 'player_name', 'team_name' columns.

    Returns
    -------
    dict with keys 'by_id' and 'by_name'
    """
    by_id = {}
    by_name = {}
    for _, row in rosters_df.iterrows():
        pid = str(row.get("player_id", "")).strip()
        name = str(row.get("player_name", "")).strip().lower()
        team = str(row.get("team_name", ""))
        if pid:
            by_id[pid] = team
        if name:
            by_name[name] = team
    return {"by_id": by_id, "by_name": by_name}


def zscores_to_json(zscores_df: pd.DataFrame, ownership_map: dict, system_key: str) -> dict:
    """
    Convert the Z-scores DataFrame to a JSON-serializable dict.

    Parameters
    ----------
    zscores_df : pd.DataFrame
        Output from compute_all_zscores().
    ownership_map : dict
        Maps lowercase player names to LORG team names.
    system_key : str
        Projection system identifier (e.g., "oopsy", "steamer").

    Returns
    -------
    dict
        JSON-ready dict with "meta" and "players" keys.
    """
    players = []
    for rank, (_, row) in enumerate(zscores_df.iterrows(), start=1):
        name = str(row.get("display_name", ""))
        # Try matching by Fantrax ID first (most reliable), then fall back to name
        fantrax_id = str(row.get("fantrax_id", "") or "").strip()
        owner = ownership_map["by_id"].get(fantrax_id) or ownership_map["by_name"].get(name.strip().lower())

        player = {
            "rank": rank,
            "name": name,
            "team": str(row.get("mlb_team", "")),
            "pos": "DH/SP" if name == "Shohei Ohtani" else str(row.get("positions", "")),
            "owner": owner,
            # Aggregate Z-scores
            "total_z": round(float(row.get("total_z", 0) or 0), 2),
            "hit_z": round(float(row.get("hit_z", 0) or 0), 2),
            "pit_z": round(float(row.get("pit_z", 0) or 0), 2),
            # Individual category Z-scores
            "r_z": round(float(row.get("r_z", 0) or 0), 2),
            "hr_z": round(float(row.get("hr_z", 0) or 0), 2),
            "rbi_z": round(float(row.get("rbi_z", 0) or 0), 2),
            "tb_z": round(float(row.get("tb_z", 0) or 0), 2),
            "obp_z": round(float(row.get("obp_z", 0) or 0), 2),
            "nsb_z": round(float(row.get("nsb_z", 0) or 0), 2),
            "ip_z": round(float(row.get("ip_z", 0) or 0), 2),
            "whip_z": round(float(row.get("whip_z", 0) or 0), 2),
            "k_z": round(float(row.get("k_z", 0) or 0), 2),
            "era_z": round(float(row.get("era_z", 0) or 0), 2),
            "qa3_z": round(float(row.get("qa3_z", 0) or 0), 2),
            "nsv_z": round(float(row.get("nsv_z", 0) or 0), 2),
            # Raw projected stats
            "proj_pa": _num(row.get("proj_pa"), 0),
            "proj_r": _num(row.get("proj_r"), 0),
            "proj_hr": _num(row.get("proj_hr"), 0),
            "proj_rbi": _num(row.get("proj_rbi"), 0),
            "proj_tb": _num(row.get("proj_tb"), 0),
            "proj_obp": _num(row.get("proj_obp"), 3),
            "proj_nsb": _num(row.get("proj_nsb"), 0),
            "proj_ip": _num(row.get("proj_ip"), 1),
            "proj_whip": _num(row.get("proj_whip"), 3),
            "proj_k": _num(row.get("proj_k"), 0),
            "proj_era": _num(row.get("proj_era"), 2),
            "proj_qa3": _num(row.get("proj_qa3"), 1),
            "proj_nsv": _num(row.get("proj_nsv"), 0),
        }
        # Position eligibility cleanup:
        # - RP with >85 projected IP are really starters → change to SP
        # - SP with >1 projected NSV are really relievers → change to RP
        pos = player["pos"]
        proj_ip = player.get("proj_ip") or 0
        proj_nsv = player.get("proj_nsv") or 0
        if "RP" in pos and proj_ip > 85:
            player["pos"] = pos.replace("RP", "SP")
        elif "SP" in pos and proj_nsv > 1:
            player["pos"] = pos.replace("SP", "RP")

        # Ohtani two-way adjustment: floor each pitching Z-score at 0.
        # In a daily-lineup league he's purely additive as a pitcher — his
        # negative counting-stat Z-scores (IP, K, QA3) shouldn't penalize him
        # since you'd only start him on days he pitches.
        if name == "Shohei Ohtani":
            pit_z_keys = ["ip_z", "whip_z", "k_z", "era_z", "qa3_z", "nsv_z"]
            for k in pit_z_keys:
                player[k] = max(player[k], 0)
            player["pit_z"] = round(sum(player[k] for k in pit_z_keys), 2)
            player["total_z"] = round(player["hit_z"] + player["pit_z"], 2)

        players.append(player)

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "projection_system": system_key,
            "num_players": len(players),
        },
        "players": players,
    }


def _num(val, decimals: int):
    """Safely convert a value to a rounded number, returning None if missing."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return round(float(val), decimals) if decimals > 0 else int(round(float(val)))
    except (ValueError, TypeError):
        return None


def main():
    """Main entry point: fetch data, compute Z-scores, write JSON."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- Fetch Fantrax roster data ---
    try:
        fantrax_lookup, rosters_df, num_teams = fetch_fantrax_data()
        ownership_map = build_ownership_map(rosters_df)
        logger.info(f"Ownership map: {len(ownership_map)} players rostered")
    except Exception as exc:
        logger.warning(f"Could not fetch Fantrax data: {exc}")
        logger.warning("Proceeding without roster data (all players will show as free agents)")
        fantrax_lookup = None
        rosters_df = pd.DataFrame()
        ownership_map = {}
        num_teams = settings.DEFAULT_NUM_TEAMS

    # --- Fetch OOPSY projections and compute Z-scores ---
    logger.info("Fetching OOPSY projections from FanGraphs...")
    try:
        batters_df, pitchers_df = fetch_projection_system("oopsy")
        logger.info(f"OOPSY: {len(batters_df)} batters, {len(pitchers_df)} pitchers")

        zscores_df = compute_all_zscores(
            batters_df, pitchers_df, num_teams, fantrax_lookup
        )
        logger.info(f"Computed Z-scores for {len(zscores_df)} players")

        # Trim to top 1000 players (beyond that is fringe minor leaguers)
        zscores_df = zscores_df.head(1000)
        output = zscores_to_json(zscores_df, ownership_map, "oopsy")
        output_path = os.path.join(OUTPUT_DIR, "rankings-oopsy.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        logger.info(f"Wrote {output_path} ({len(output['players'])} players)")

    except Exception as exc:
        logger.error(f"Failed to generate OOPSY rankings: {exc}", exc_info=True)
        raise

    logger.info("Done.")


if __name__ == "__main__":
    main()
