"""
fangraphs_data.py — FanGraphs Steamer 2026 projection fetcher.

Primary data source: the FanGraphs projections page. Projection data is
embedded in a __NEXT_DATA__ script tag as part of Next.js server-side
rendering. This module fetches the HTML page, extracts the JSON payload
from that script tag, and parses it into a pandas DataFrame.

No authentication is required for public projection data.

Fallback: if the page is unavailable, the Streamlit UI offers CSV upload
widgets. Uploaded files are parsed by the same cleaning functions used
for page data, ensuring consistent column names and types downstream.

Column mapping (FanGraphs page → internal names)
-------------------------------------------------
Batters:
  PlayerName → name
  playerid   → fg_playerid
  2B         → double   (digit-prefix column names are invalid in Python)
  3B         → triple
  Team       → team

Pitchers:
  PlayerName → name
  playerid   → fg_playerid
  SO         → k        (strikeouts are labelled SO on FanGraphs)
  Team       → team
"""

import io
import json
import logging
import re

import pandas as pd
import requests

from lib.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------


class FanGraphsAPIError(Exception):
    """Raised when the FanGraphs projection page is unreachable or malformed."""
    pass


# ---------------------------------------------------------------------------
# Required column definitions (used for validation)
# ---------------------------------------------------------------------------

_REQUIRED_BAT_COLS = {
    "fg_playerid", "name", "team",
    "g", "pa", "ab", "h", "double", "triple", "hr",
    "r", "rbi", "bb", "sb", "cs", "obp",
}

_REQUIRED_PIT_COLS = {
    "fg_playerid", "name", "team",
    "gs", "ip", "era", "whip", "k", "sv",
}

# ---------------------------------------------------------------------------
# Validation helper
# ---------------------------------------------------------------------------


def _validate_fg_columns(df: pd.DataFrame, required: set, source_label: str) -> None:
    """
    Check that df contains all required columns.

    Raises a clear FanGraphsAPIError (not a cryptic KeyError deep in the
    pipeline) if any required column is missing.

    Parameters
    ----------
    df : pd.DataFrame
    required : set
        Set of required column names.
    source_label : str
        Human-readable label for error messages (e.g. "batter projections").
    """
    missing = required - set(df.columns)
    if missing:
        raise FanGraphsAPIError(
            f"FanGraphs {source_label} missing expected columns: {sorted(missing)}. "
            f"Available columns: {sorted(df.columns)}. "
            "The FanGraphs page structure may have changed. "
            "Use the manual CSV upload in the sidebar as a fallback."
        )


# ---------------------------------------------------------------------------
# Column renaming maps
# ---------------------------------------------------------------------------

# FanGraphs page data uses mixed casing — check multiple forms.
# Map: possible raw name → internal name
_BAT_COL_MAP = {
    "PlayerName": "name",
    "playername": "name",
    "playerName": "name",
    "playerid":   "fg_playerid",
    "PlayerId":   "fg_playerid",
    "playerID":   "fg_playerid",
    "Team":       "team",
    "TEAM":       "team",
    "2B":         "double",
    "3B":         "triple",
    "G":          "g",
    "PA":         "pa",
    "AB":         "ab",
    "H":          "h",
    "HR":         "hr",
    "R":          "r",
    "RBI":        "rbi",
    "BB":         "bb",
    "HBP":        "hbp",
    "SF":         "sf",
    "SB":         "sb",
    "CS":         "cs",
    "OBP":        "obp",
}

_PIT_COL_MAP = {
    "PlayerName": "name",
    "playername": "name",
    "playerName": "name",
    "playerid":   "fg_playerid",
    "PlayerId":   "fg_playerid",
    "playerID":   "fg_playerid",
    "Team":       "team",
    "TEAM":       "team",
    "G":          "g",
    "GS":         "gs",
    "IP":         "ip",
    "ERA":        "era",
    "WHIP":       "whip",
    "SO":         "k",
    "K":          "k",
    "SV":         "sv",
    "BS":         "bs",
}


def _apply_col_map(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    """Rename columns using a mapping dict; ignores keys not in df."""
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    return df.rename(columns=rename)


# ---------------------------------------------------------------------------
# __NEXT_DATA__ extraction
# ---------------------------------------------------------------------------

# Regex to find the __NEXT_DATA__ script tag in the HTML page.
# The data is a single JSON object inside <script id="__NEXT_DATA__" ...>...</script>.
_NEXT_DATA_RE = re.compile(
    r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
    re.DOTALL,
)


def _fetch_next_data(url: str, label: str) -> list[dict]:
    """
    Fetch a FanGraphs projections page and extract the player data array
    from the embedded __NEXT_DATA__ JSON payload.

    How it works:
    1. GET the HTML page with a browser-like User-Agent.
    2. Find the <script id="__NEXT_DATA__"> tag via regex.
    3. Parse the JSON payload inside that tag.
    4. Navigate to: props → pageProps → dehydratedState → queries[0] → state → data
    5. Return the list of player dictionaries.

    Parameters
    ----------
    url : str
        Full URL to the FanGraphs projections page.
    label : str
        Human-readable label for error messages (e.g. "batter projections").

    Returns
    -------
    list[dict]
        Each dict is one player's projection data with FanGraphs column names.

    Raises
    ------
    FanGraphsAPIError
        If the page can't be fetched, parsed, or doesn't contain the expected data.
    """
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        logger.info(f"Fetching FanGraphs {label} page: {url}")
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        html = response.text
        logger.info(f"FanGraphs {label} page response: status={response.status_code}, length={len(html)}")
    except requests.exceptions.RequestException as exc:
        raise FanGraphsAPIError(
            f"Could not reach FanGraphs {label} page: {exc}"
        ) from exc

    # Step 2: Extract __NEXT_DATA__ JSON from the HTML
    match = _NEXT_DATA_RE.search(html)
    if not match:
        raise FanGraphsAPIError(
            f"Could not find __NEXT_DATA__ script tag on FanGraphs {label} page. "
            "The page structure may have changed. "
            "Use the manual CSV upload in the sidebar as a fallback."
        )

    try:
        next_data = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise FanGraphsAPIError(
            f"Could not parse __NEXT_DATA__ JSON from FanGraphs {label} page: {exc}"
        ) from exc

    # Step 3: Navigate to the projection data array
    # Path: props.pageProps.dehydratedState.queries[0].state.data
    try:
        queries = next_data["props"]["pageProps"]["dehydratedState"]["queries"]
        if not queries:
            raise KeyError("queries array is empty")
        data = queries[0]["state"]["data"]
    except (KeyError, IndexError, TypeError) as exc:
        raise FanGraphsAPIError(
            f"Unexpected __NEXT_DATA__ structure for FanGraphs {label}. "
            f"Could not find projection data at expected path: {exc}. "
            "The page structure may have changed. "
            "Use the manual CSV upload as a fallback."
        ) from exc

    if not data or not isinstance(data, list):
        raise FanGraphsAPIError(
            f"FanGraphs {label} page returned empty projection data. "
            "2026 Steamer projections may not be available yet."
        )

    logger.info(
        f"Extracted {len(data)} {label} records from FanGraphs __NEXT_DATA__. "
        f"Columns in first record: {sorted(data[0].keys()) if data else 'N/A'}"
    )
    return data


# ---------------------------------------------------------------------------
# Batter projections
# ---------------------------------------------------------------------------


def fetch_batter_projections(url: str = None, label: str = "batter projections") -> pd.DataFrame:
    """
    Fetch batter projections from a FanGraphs projections page.

    The data is extracted from the __NEXT_DATA__ script tag embedded in the
    server-rendered HTML (FanGraphs uses Next.js).

    Parameters
    ----------
    url : str, optional
        Full URL to the FanGraphs batter projections page.  Defaults to
        settings.FG_BAT_URL (Steamer).
    label : str
        Human-readable label for log/error messages.

    Returns
    -------
    pd.DataFrame
        Cleaned batter projections with columns matching _REQUIRED_BAT_COLS
        plus optional columns (hbp, sf). Sorted by PA descending.

    Raises
    ------
    FanGraphsAPIError
        If the page is unreachable, unparseable, or missing expected columns.
    """
    if url is None:
        url = settings.FG_BAT_URL
    raw_records = _fetch_next_data(url, label)
    df = pd.DataFrame(raw_records)
    return _clean_batter_df(df)


def _clean_batter_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns, fill missing values, and enforce data types for batters.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame from FanGraphs page data or uploaded CSV.

    Returns
    -------
    pd.DataFrame
    """
    df = _apply_col_map(df, _BAT_COL_MAP)

    _validate_fg_columns(df, _REQUIRED_BAT_COLS, "batter projections")

    # Ensure optional columns exist (fill with 0 if absent)
    for col in ["hbp", "sf"]:
        if col not in df.columns:
            df[col] = 0.0

    # Select and order columns
    keep = ["fg_playerid", "name", "team", "g", "pa", "ab", "h", "double", "triple",
            "hr", "r", "rbi", "bb", "hbp", "sf", "sb", "cs", "obp"]
    df = df[keep].copy()

    # Convert all numeric columns to float; coerce errors to NaN then fill 0
    numeric_cols = [c for c in keep if c not in ("fg_playerid", "name", "team")]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    # OBP: leave NaN if it's genuinely missing (flag for investigation)
    # All other numeric cols: fill NaN with 0
    other_numeric = [c for c in numeric_cols if c != "obp"]
    df[other_numeric] = df[other_numeric].fillna(0.0)

    # Ensure player ID is a string
    df["fg_playerid"] = df["fg_playerid"].astype(str)

    # Noise filter: drop players with < 10 PA (not meaningfully projectable)
    df = df[df["pa"] >= 10].copy()

    # Sort by PA descending (most rosterable players first)
    df = df.sort_values("pa", ascending=False).reset_index(drop=True)

    logger.info(f"Loaded {len(df)} batter projections from FanGraphs.")
    return df


# ---------------------------------------------------------------------------
# Pitcher projections
# ---------------------------------------------------------------------------


def fetch_pitcher_projections(url: str = None, label: str = "pitcher projections") -> pd.DataFrame:
    """
    Fetch pitcher projections from a FanGraphs projections page.

    Parameters
    ----------
    url : str, optional
        Full URL to the FanGraphs pitcher projections page.  Defaults to
        settings.FG_PIT_URL (Steamer).
    label : str
        Human-readable label for log/error messages.

    Returns
    -------
    pd.DataFrame
        Cleaned pitcher projections. Sorted by IP descending.

    Raises
    ------
    FanGraphsAPIError
        If the page is unreachable or returns unexpected data.
    """
    if url is None:
        url = settings.FG_PIT_URL
    raw_records = _fetch_next_data(url, label)
    df = pd.DataFrame(raw_records)
    return _clean_pitcher_df(df)


def _clean_pitcher_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns, fill missing values, and enforce data types for pitchers.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame from FanGraphs page data or uploaded CSV.

    Returns
    -------
    pd.DataFrame
    """
    df = _apply_col_map(df, _PIT_COL_MAP)

    _validate_fg_columns(df, _REQUIRED_PIT_COLS, "pitcher projections")

    # BS (blown saves) may not be in every year's projection data
    if "bs" not in df.columns:
        logger.info("BS column not found in pitcher projections. Estimating bs = sv * 0.18")
        df["bs"] = (df.get("sv", 0) * 0.18).round(1)

    # g (total games) may be absent for some export types
    if "g" not in df.columns:
        df["g"] = df.get("gs", 0)

    keep = ["fg_playerid", "name", "team", "g", "gs", "ip", "era", "whip", "k", "sv", "bs"]
    df = df[keep].copy()

    numeric_cols = [c for c in keep if c not in ("fg_playerid", "name", "team")]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    df[numeric_cols] = df[numeric_cols].fillna(0.0)

    df["fg_playerid"] = df["fg_playerid"].astype(str)

    # Noise filter: drop pitchers with < 1 IP
    df = df[df["ip"] >= 1.0].copy()

    df = df.sort_values("ip", ascending=False).reset_index(drop=True)

    logger.info(f"Loaded {len(df)} pitcher projections from FanGraphs.")
    return df


# ---------------------------------------------------------------------------
# CSV upload fallback
# ---------------------------------------------------------------------------


def load_batter_csv(uploaded_file) -> pd.DataFrame:
    """
    Parse a user-uploaded FanGraphs batter projections CSV.

    The CSV should be a standard FanGraphs export. The same column renaming
    and cleaning logic used for page data is applied here.

    Parameters
    ----------
    uploaded_file : streamlit.runtime.uploaded_file_manager.UploadedFile
        File object from st.file_uploader().

    Returns
    -------
    pd.DataFrame
        Cleaned batter projections (same schema as fetch_batter_projections).

    Raises
    ------
    FanGraphsAPIError
        If the CSV cannot be parsed or is missing required columns.
    """
    try:
        content = uploaded_file.read()
        df = pd.read_csv(io.BytesIO(content))
    except Exception as exc:
        raise FanGraphsAPIError(f"Could not read batter CSV: {exc}") from exc
    return _clean_batter_df(df)


def load_pitcher_csv(uploaded_file) -> pd.DataFrame:
    """
    Parse a user-uploaded FanGraphs pitcher projections CSV.

    Parameters
    ----------
    uploaded_file : streamlit.runtime.uploaded_file_manager.UploadedFile

    Returns
    -------
    pd.DataFrame
        Cleaned pitcher projections (same schema as fetch_pitcher_projections).

    Raises
    ------
    FanGraphsAPIError
        If the CSV cannot be parsed or is missing required columns.
    """
    try:
        content = uploaded_file.read()
        df = pd.read_csv(io.BytesIO(content))
    except Exception as exc:
        raise FanGraphsAPIError(f"Could not read pitcher CSV: {exc}") from exc
    return _clean_pitcher_df(df)


# ---------------------------------------------------------------------------
# Convenience: fetch or load from session state
# ---------------------------------------------------------------------------


def fetch_projection_system(system_key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch both batter and pitcher projections for a named projection system.

    Parameters
    ----------
    system_key : str
        Key into settings.PROJECTION_SYSTEMS (e.g. "steamer", "oopsy").

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (batters_df, pitchers_df) — cleaned and validated.

    Raises
    ------
    FanGraphsAPIError
        If the system_key is unknown or either fetch fails.
    """
    system = settings.PROJECTION_SYSTEMS.get(system_key)
    if system is None:
        raise FanGraphsAPIError(
            f"Unknown projection system '{system_key}'. "
            f"Valid options: {list(settings.PROJECTION_SYSTEMS.keys())}"
        )

    label = system["label"]
    bat_url = system["bat_url"]
    pit_url = system["pit_url"]

    logger.info(f"Fetching {label} batter projections from {bat_url}")
    batters_df = fetch_batter_projections(url=bat_url, label=f"{label} batters")

    logger.info(f"Fetching {label} pitcher projections from {pit_url}")
    pitchers_df = fetch_pitcher_projections(url=pit_url, label=f"{label} pitchers")

    return batters_df, pitchers_df


def get_batter_projections(session_state: dict) -> pd.DataFrame:
    """
    Return batter projections from manual session-state upload if available,
    otherwise fetch from FanGraphs.

    Parameters
    ----------
    session_state : dict
        Streamlit st.session_state (or equivalent dict).

    Returns
    -------
    pd.DataFrame

    Raises
    ------
    FanGraphsAPIError
        Propagated from fetch_batter_projections() if fetch fails.
    """
    if session_state.get("manual_batters_df") is not None:
        logger.info("Using manually uploaded batter projections.")
        return session_state["manual_batters_df"]
    return fetch_batter_projections()


def get_pitcher_projections(session_state: dict) -> pd.DataFrame:
    """
    Return pitcher projections from manual session-state upload if available,
    otherwise fetch from FanGraphs.

    Parameters
    ----------
    session_state : dict

    Returns
    -------
    pd.DataFrame
    """
    if session_state.get("manual_pitchers_df") is not None:
        logger.info("Using manually uploaded pitcher projections.")
        return session_state["manual_pitchers_df"]
    return fetch_pitcher_projections()
