"""
fantrax_api.py — Fantrax REST API client and data orchestration.

All HTTP calls to the Fantrax API go through this module.
Returns clean Python dicts or pandas DataFrames; never touches DuckDB directly.

Official API base URL: https://www.fantrax.com/fxea/general/
Documentation:         https://www.fantrax.com/developer

Endpoints used
--------------
getLeagueInfo   — team names/IDs, league config
getTeamRosters  — all player roster slots with status (ACTIVE / RESERVE / MINORS)
getPlayerIds    — player ID-to-name lookup (keyed by sport, e.g. sport=MLB)

Fantrax API response structure (verified live)
----------------------------------------------
getLeagueInfo:
  - teamInfo: dict keyed by team_id → {"name": str, "id": str}
  - matchups: list of period matchup data (contains team names too)
  - playerInfo: dict keyed by player_id → {"eligiblePos": str, "status": str}
  - rosterInfo: {"positionConstraints": {...}, "maxTotalPlayers": int, ...}

getTeamRosters:
  - rosters: dict keyed by team_id → {"teamName": str, "rosterItems": [...]}
  - Each rosterItem: {"id": str, "position": str, "status": str}
  - NOTE: roster items do NOT contain player names — must join with getPlayerIds

getPlayerIds:
  - Dict keyed by player short ID → {"name": str, "fantraxId": str,
    "position": str, "team": str, ...}
  - Param: sport=MLB (not leagueId)
"""

import logging
import time
from typing import Optional

import pandas as pd
import requests

from lib.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class FantraxAPIError(Exception):
    """Raised when the Fantrax API returns an error or is unreachable."""
    pass


# ---------------------------------------------------------------------------
# Internal HTTP helper
# ---------------------------------------------------------------------------

_SESSION = requests.Session()
_SESSION.headers.update({"Accept": "application/json"})


def _get(endpoint: str, params: Optional[dict] = None) -> dict:
    """
    Make a GET request to the Fantrax API.

    Retries up to 3 times with 2-second backoff on connection/timeout errors.

    Parameters
    ----------
    endpoint : str
        Endpoint name (e.g. "getLeagueInfo"). Appended to FANTRAX_BASE_URL.
    params : dict, optional
        Query string parameters.

    Returns
    -------
    dict
        Parsed JSON response body.

    Raises
    ------
    FantraxAPIError
        On HTTP 4xx/5xx, JSON parse failure, or after all retries exhausted.
    """
    url = settings.FANTRAX_BASE_URL + endpoint
    max_retries = 3
    backoff = 2  # seconds

    for attempt in range(1, max_retries + 1):
        try:
            response = _SESSION.get(url, params=params, timeout=15)
            response.raise_for_status()
            # Force UTF-8 decoding to handle smart quotes and accented characters
            response.encoding = "utf-8"
            data = response.json()

            # Some Fantrax endpoints return HTTP 200 with an error payload
            if isinstance(data, dict) and data.get("error"):
                err = data["error"]
                raise FantraxAPIError(
                    f"Fantrax API error on {endpoint}: {err.get('message', err)}"
                )
            if isinstance(data, dict) and data.get("errors"):
                raise FantraxAPIError(
                    f"Fantrax API error on {endpoint}: {data['errors']}"
                )

            return data

        except requests.exceptions.Timeout:
            if attempt == max_retries:
                raise FantraxAPIError(
                    f"Fantrax API timed out after {max_retries} attempts: {endpoint}"
                )
            logger.warning(f"Fantrax API timeout (attempt {attempt}), retrying...")
            time.sleep(backoff)

        except requests.exceptions.ConnectionError:
            if attempt == max_retries:
                raise FantraxAPIError(
                    f"Fantrax API connection failed after {max_retries} attempts: {endpoint}"
                )
            logger.warning(f"Fantrax API connection error (attempt {attempt}), retrying...")
            time.sleep(backoff)

        except requests.exceptions.HTTPError as exc:
            raise FantraxAPIError(
                f"Fantrax API HTTP {exc.response.status_code} on {endpoint}: {exc}"
            ) from exc

        except ValueError as exc:
            raise FantraxAPIError(
                f"Fantrax API returned non-JSON response from {endpoint}: {exc}"
            ) from exc


# ---------------------------------------------------------------------------
# Public API functions
# ---------------------------------------------------------------------------


def get_league_info(league_id: str = settings.LEAGUE_ID) -> dict:
    """
    Fetch league metadata: team names/IDs and league configuration.

    Returns
    -------
    dict
        Raw Fantrax response containing teamInfo, rosterInfo, matchups, etc.
    """
    return _get("getLeagueInfo", {"leagueId": league_id})


def get_team_rosters(
    league_id: str = settings.LEAGUE_ID,
    period: int = 0,
) -> dict:
    """
    Fetch all team rosters for a given lineup period.

    Returns
    -------
    dict
        Raw Fantrax response with rosters dict keyed by team_id.
    """
    params = {"leagueId": league_id}
    if period:
        params["period"] = period
    return _get("getTeamRosters", params)


def get_player_ids(sport: str = "MLB") -> dict:
    """
    Fetch the master player ID lookup table from Fantrax.

    This is the only endpoint that provides player names. It returns ALL
    players known to Fantrax for the given sport, keyed by their short ID.

    Parameters
    ----------
    sport : str
        Sport code. Default "MLB".

    Returns
    -------
    dict
        Raw response: dict keyed by player short ID → player info dict.
        Each value has at least: name, fantraxId, position, team.
    """
    return _get("getPlayerIds", {"sport": sport})


# ---------------------------------------------------------------------------
# Parsing helpers — convert raw API responses to DataFrames
# ---------------------------------------------------------------------------


def _normalize_positions(pos_value) -> str:
    """
    Normalize Fantrax position data to a comma-separated string.

    The Fantrax API may return positions as:
    - A comma-separated string: "2B,UT,SS"
    - A slash-delimited string: "SS/2B"
    - A single string: "SP"
    - None / missing

    The special "UT" (utility) position is stripped since it just means
    the player can be placed in a Util slot.

    Returns
    -------
    str
        Comma-separated positions, e.g. "SS,2B"
    """
    if pos_value is None:
        return ""
    if isinstance(pos_value, list):
        positions = [str(p).strip() for p in pos_value if p]
    else:
        # Handle both comma and slash delimiters
        positions = [p.strip() for p in str(pos_value).replace("/", ",").split(",") if p.strip()]

    # Remove the UT (utility) pseudo-position — it's a roster slot, not a real position
    positions = [p for p in positions if p.upper() != "UT"]
    return ",".join(positions)


def _normalize_status(raw_status: str) -> str:
    """
    Map Fantrax status strings to our internal status names.

    Fantrax uses:  ACTIVE, RESERVE, MINORS, IR, IR-LT
    We normalize to: Active, Reserve, Minors, IL
    """
    mapping = {
        "ACTIVE": "Active",
        "RESERVE": "Reserve",
        "MINORS": "Minors",
        "IR": "IL",
        "IR-LT": "IL",
        "INJURED_RESERVE": "IL",
    }
    return mapping.get(raw_status.upper().strip(), raw_status)


def parse_league_info(raw: dict) -> tuple:
    """
    Extract team list from a getLeagueInfo response.

    The actual Fantrax response has:
    - teamInfo: dict keyed by team_id → {"name": "...", "id": "..."}
    - No separate "numTeams" field — we count the teams.

    Parameters
    ----------
    raw : dict
        Raw response from get_league_info().

    Returns
    -------
    tuple[pd.DataFrame, int]
        (teams_df, num_teams)
        teams_df has columns: team_id, team_name, owner_name
    """
    team_info = raw.get("teamInfo", {})

    rows = []
    for team_id, team_data in team_info.items():
        if isinstance(team_data, dict):
            rows.append({
                "team_id":    str(team_data.get("id", team_id)),
                "team_name":  str(team_data.get("name", "Unknown")),
                "owner_name": str(team_data.get("ownerName", "")),
            })

    num_teams = len(rows)

    # Validate num_teams
    lo, hi = settings.NUM_TEAMS_VALID_RANGE
    if not (lo <= num_teams <= hi):
        logger.warning(
            f"Unexpected team count from Fantrax API: {num_teams}. "
            f"Defaulting to {settings.DEFAULT_NUM_TEAMS}."
        )
        num_teams = settings.DEFAULT_NUM_TEAMS

    teams_df = pd.DataFrame(rows, columns=["team_id", "team_name", "owner_name"])
    logger.info(f"Parsed {len(teams_df)} teams from Fantrax league info.")
    return teams_df, num_teams


def build_player_name_lookup() -> dict:
    """
    Fetch the Fantrax player ID directory and build a lookup dict.

    Returns
    -------
    dict
        {player_short_id: {"name": str, "team": str, "position": str, "fantraxId": str}}
    """
    try:
        raw = get_player_ids("MLB")
    except FantraxAPIError:
        logger.warning("Could not fetch Fantrax player IDs. Player names will be missing.")
        return {}

    # The response is a dict keyed by short player ID
    if not isinstance(raw, dict):
        return {}

    lookup = {}
    for pid, pdata in raw.items():
        if isinstance(pdata, dict) and "name" in pdata:
            lookup[pid] = {
                "name": pdata.get("name", ""),
                "team": pdata.get("team", ""),
                "position": pdata.get("position", ""),
                "fantraxId": pdata.get("fantraxId", pid),
            }

    logger.info(f"Built Fantrax player name lookup: {len(lookup)} players.")
    return lookup


def parse_team_rosters(raw: dict, player_lookup: dict = None) -> pd.DataFrame:
    """
    Extract per-team roster data from a getTeamRosters response.

    The actual Fantrax response has:
    - rosters: dict keyed by team_id → {"teamName": str, "rosterItems": [...]}
    - Each rosterItem: {"id": str, "position": str, "status": str}
    - Player names come from the player_lookup (getPlayerIds result).

    Parameters
    ----------
    raw : dict
        Raw response from get_team_rosters().
    player_lookup : dict, optional
        From build_player_name_lookup(). If None, player names will be IDs.

    Returns
    -------
    pd.DataFrame
        Columns: team_id, player_id, player_name, positions, status, period
    """
    if player_lookup is None:
        player_lookup = {}

    rosters_data = raw.get("rosters", {})
    period = raw.get("period", 0)

    rows = []

    if isinstance(rosters_data, dict):
        for team_id, team_data in rosters_data.items():
            if not isinstance(team_data, dict):
                continue

            roster_items = team_data.get("rosterItems", [])
            for player in roster_items:
                if not isinstance(player, dict):
                    continue

                pid = str(player.get("id", ""))
                raw_status = str(player.get("status", "Active"))
                # roster_position is the actual lineup slot (e.g., "SS", "SP",
                # "BN" for bench, "Util", etc.). This is different from
                # eligible positions which lists all positions a player qualifies at.
                roster_position = str(player.get("position", "")).strip()

                # Look up player info from the master player list
                player_info = player_lookup.get(pid, {})
                player_name = player_info.get("name", pid)  # fallback to ID
                # Use eligible positions from the lookup (more complete than roster position)
                eligible_pos = player_info.get("position", roster_position)
                mlb_team = player_info.get("team", "")

                # Fantrax names are "Last, First" — convert to "First Last"
                if "," in player_name:
                    parts = player_name.split(",", 1)
                    player_name = f"{parts[1].strip()} {parts[0].strip()}"

                rows.append({
                    "team_id":     str(team_id),
                    "player_id":   pid,
                    "player_name": player_name,
                    "mlb_team":    mlb_team,
                    "positions":   _normalize_positions(eligible_pos),
                    "roster_slot": roster_position,
                    "status":      _normalize_status(raw_status),
                    "period":      int(period),
                })

    df = pd.DataFrame(rows, columns=["team_id", "player_id", "player_name", "mlb_team", "positions", "roster_slot", "status", "period"])
    logger.info(f"Parsed {len(df)} roster entries across {df['team_id'].nunique()} teams.")
    return df


# ---------------------------------------------------------------------------
# Top-level data refresh orchestrator
# ---------------------------------------------------------------------------


def refresh_fantrax_data(conn) -> dict:
    """
    Fetch fresh Fantrax data and write it to the DuckDB cache.

    Called from the Streamlit sidebar "Refresh Data" button.

    Steps:
    1. Fetch player name directory (getPlayerIds?sport=MLB)
    2. Fetch league info (getLeagueInfo) → teams
    3. Fetch team rosters (getTeamRosters) → roster slots with player names

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Active DuckDB connection (from st.session_state).

    Returns
    -------
    dict
        Status messages per data source.
    """
    status = {}

    # --- Player name directory (needed to resolve roster IDs to names) ---
    player_lookup = {}
    try:
        player_lookup = build_player_name_lookup()
        logger.info(f"Player name lookup: {len(player_lookup)} entries")
    except Exception as exc:
        logger.warning(f"Could not build player name lookup: {exc}")

    # --- League info (teams) ---
    try:
        raw_info = get_league_info()
        teams_df, num_teams = parse_league_info(raw_info)
        database.write_dataframe(conn, "fantrax_teams", teams_df)
        status["fantrax_teams"] = f"OK ({len(teams_df)} teams)"
        logger.info(f"Loaded {len(teams_df)} teams from Fantrax (numTeams={num_teams})")
    except FantraxAPIError as exc:
        status["fantrax_teams"] = f"Error: {exc}"
        logger.error(f"Failed to fetch Fantrax teams: {exc}")

    # --- Team rosters ---
    try:
        raw_rosters = get_team_rosters()
        rosters_df = parse_team_rosters(raw_rosters, player_lookup)
        database.write_dataframe(conn, "fantrax_rosters", rosters_df)
        status["fantrax_rosters"] = f"OK ({len(rosters_df)} roster slots)"
        logger.info(f"Loaded {len(rosters_df)} roster slots from Fantrax")
    except FantraxAPIError as exc:
        status["fantrax_rosters"] = f"Error: {exc}"
        logger.error(f"Failed to fetch Fantrax rosters: {exc}")

    return status


def get_num_teams_from_cache(conn) -> int:
    """
    Return the number of teams based on cached Fantrax team data.
    Falls back to DEFAULT_NUM_TEAMS if the cache is empty.
    """
    try:
        teams_df = database.read_table(conn, "fantrax_teams")
        if teams_df.empty:
            return settings.DEFAULT_NUM_TEAMS
        count = len(teams_df)
        lo, hi = settings.NUM_TEAMS_VALID_RANGE
        if lo <= count <= hi:
            return count
    except Exception:
        pass
    return settings.DEFAULT_NUM_TEAMS
