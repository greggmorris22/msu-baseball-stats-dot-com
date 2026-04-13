"""
player_matching.py — Player identity resolution across data sources.

FanGraphs and Fantrax use different player IDs and sometimes different team
abbreviations. This module bridges the two namespaces so that Z-scores
(computed from FanGraphs projections) can be linked back to a player's
Fantrax roster slot.

Matching strategy
-----------------
1. If the pybaseball Chadwick register is available, build a crosswalk
   from FanGraphs player IDs to names, then match against Fantrax names.
2. Exact match on normalized_name + normalized_team_abbrev.
3. Fuzzy name match via difflib (cutoff = 0.85), tiebroken by team.
4. No match → fantrax_id = None, flagged for manual review.

Known limitations
-----------------
- Players traded mid-season may have a stale team in Fantrax rosters.
  Name matching is prioritised over team for high-confidence fuzzy matches.
- Two players with the same name on different teams (e.g., Luis Garcia)
  are disambiguated by team and position.
"""

import difflib
import unicodedata
import re
import logging
from typing import Optional

import pandas as pd

# pybaseball is optional — used for the Chadwick player ID crosswalk
try:
    from pybaseball import chadwick_register
    _HAS_PYBASEBALL = True
except ImportError:
    _HAS_PYBASEBALL = False

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Team abbreviation crosswalk
# FanGraphs → Fantrax (add more as discovered from live data)
# ---------------------------------------------------------------------------
_FG_TO_FANTRAX_TEAM = {
    "ARI": "AZ",    # Arizona Diamondbacks
    "CWS": "CHW",   # Chicago White Sox
    "TB":  "TBR",   # Tampa Bay Rays
    "WSN": "WAS",   # Washington Nationals
    "MIA": "FLA",   # Miami Marlins (legacy abbreviation sometimes used)
    "SF":  "SFG",   # San Francisco Giants
    "SD":  "SDP",   # San Diego Padres
    "KC":  "KCR",   # Kansas City Royals
    "NYY": "NYY",   # identical — listed for completeness
    "BOS": "BOS",
}

# Inverse map (Fantrax → FanGraphs)
_FANTRAX_TO_FG_TEAM = {v: k for k, v in _FG_TO_FANTRAX_TEAM.items()}


def normalize_team(team: str, to: str = "fg") -> str:
    """
    Normalize a team abbreviation to a common form.

    Parameters
    ----------
    team : str
        Raw team abbreviation from either FanGraphs or Fantrax.
    to : str
        Target namespace: "fg" (FanGraphs) or "fantrax".

    Returns
    -------
    str
        Normalized abbreviation (uppercase). Returns the original if no
        mapping is found — many abbreviations are identical across platforms.
    """
    team = (team or "").strip().upper()
    if to == "fantrax":
        return _FG_TO_FANTRAX_TEAM.get(team, team)
    else:  # to == "fg"
        return _FANTRAX_TO_FG_TEAM.get(team, team)


# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------

# Suffixes that should be stripped from player names before matching
_SUFFIXES = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b\.?", re.IGNORECASE)


def normalize_name(name: str) -> str:
    """
    Normalize a player name for fuzzy/exact matching.

    Steps:
    1. Decompose unicode characters (NFD) and strip accent marks.
    2. Convert to ASCII (ignore non-ASCII characters).
    3. Lowercase.
    4. Remove suffixes (Jr., Sr., II, III, etc.).
    5. Remove punctuation and extra whitespace.

    Examples
    --------
    "Shohei Ohtani"   → "shohei ohtani"
    "José Abreu"      → "jose abreu"
    "Sandy Alcantara" → "sandy alcantara"
    "Freddy Peralta Jr." → "freddy peralta"

    Parameters
    ----------
    name : str

    Returns
    -------
    str
    """
    if not name:
        return ""
    # Remove accents
    nfd = unicodedata.normalize("NFD", str(name))
    ascii_name = nfd.encode("ascii", "ignore").decode("ascii")
    # Lowercase
    lower = ascii_name.lower()
    # Remove suffixes
    stripped = _SUFFIXES.sub("", lower)
    # Remove punctuation (keep spaces and alphanumerics)
    clean = re.sub(r"[^a-z0-9 ]", "", stripped)
    # Collapse whitespace
    return " ".join(clean.split())


def _make_lookup_key(name: str, team: str) -> str:
    """Combine normalized name and team into a lookup key."""
    return f"{normalize_name(name)}|{(team or '').strip().upper()}"


# ---------------------------------------------------------------------------
# Fantrax lookup table construction
# ---------------------------------------------------------------------------


def build_fantrax_lookup(fantrax_players_df: pd.DataFrame) -> dict:
    """
    Build a dict mapping normalized player identity keys to Fantrax player info.

    The key format is: "{normalized_name}|{team_abbrev}"

    Each value is a dict with 'player_id' and 'positions' so that the
    matching step can carry position eligibility back to the Z-scores table.

    Parameters
    ----------
    fantrax_players_df : pd.DataFrame
        Must have columns: player_id, player_name, positions
        and one of: mlb_team or team.

    Returns
    -------
    dict
        {lookup_key: {"player_id": str, "positions": str}}
    """
    lookup = {}
    team_col = "mlb_team" if "mlb_team" in fantrax_players_df.columns else "team"

    for _, row in fantrax_players_df.iterrows():
        team = normalize_team(str(row.get(team_col, "")), to="fg")
        key = _make_lookup_key(str(row["player_name"]), team)
        entry = {
            "player_id": str(row["player_id"]),
            "positions": str(row.get("positions", "")),
        }
        lookup[key] = entry

        # Also index with the raw Fantrax team abbreviation as a fallback
        raw_key = _make_lookup_key(str(row["player_name"]), str(row.get(team_col, "")))
        lookup.setdefault(raw_key, entry)

    return lookup


# ---------------------------------------------------------------------------
# pybaseball Chadwick crosswalk (FanGraphs ID → name lookup)
# ---------------------------------------------------------------------------

_chadwick_cache: Optional[pd.DataFrame] = None


def get_chadwick_fg_names() -> dict:
    """
    Load the Chadwick register from pybaseball and build a mapping of
    FanGraphs player ID (as string) → full name.

    This is used to enrich FanGraphs projection data with authoritative
    names that may better match Fantrax's naming conventions.

    The register is cached in a module-level variable so it's only
    downloaded once per session.

    Returns
    -------
    dict
        {fg_player_id_str: "First Last"} for all players with a key_fangraphs.
        Returns an empty dict if pybaseball is not installed.
    """
    global _chadwick_cache

    if not _HAS_PYBASEBALL:
        logger.info("pybaseball not available; skipping Chadwick crosswalk.")
        return {}

    if _chadwick_cache is None:
        try:
            logger.info("Loading Chadwick player register from pybaseball...")
            _chadwick_cache = chadwick_register()
            logger.info(f"Chadwick register loaded: {len(_chadwick_cache)} entries.")
        except Exception as exc:
            logger.warning(f"Could not load Chadwick register: {exc}")
            return {}

    reg = _chadwick_cache
    # Filter to players with a FanGraphs ID
    fg_rows = reg[reg["key_fangraphs"].notna() & (reg["key_fangraphs"] != "")].copy()

    lookup = {}
    for _, row in fg_rows.iterrows():
        fg_id = str(row["key_fangraphs"])
        first = str(row.get("name_first", "")).strip()
        last = str(row.get("name_last", "")).strip()
        if first and last:
            lookup[fg_id] = f"{first} {last}"

    logger.info(f"Chadwick FanGraphs name crosswalk: {len(lookup)} players.")
    return lookup


# ---------------------------------------------------------------------------
# Main matching function
# ---------------------------------------------------------------------------


def match_fg_to_fantrax(
    fg_df: pd.DataFrame,
    fantrax_lookup: dict,
    threshold: float = 0.85,
) -> pd.DataFrame:
    """
    Match FanGraphs players to Fantrax player IDs.

    For each player in fg_df, attempts:
    1. Exact match on normalized_name|team key.
    2. Fuzzy match on name only (difflib), with team used as tiebreaker.
    3. If no match, fantrax_id = None.

    All fuzzy matches and unmatched players are logged at INFO level so
    the user can review them in the console.

    Parameters
    ----------
    fg_df : pd.DataFrame
        Must have columns: name, team, fg_playerid.
    fantrax_lookup : dict
        Built by build_fantrax_lookup().
    threshold : float
        Minimum similarity score (0–1) for fuzzy matching. Default 0.85.

    Returns
    -------
    pd.DataFrame
        fg_df with an added 'fantrax_id' column (str or None).
    """
    fg_df = fg_df.copy()
    fantrax_ids = []
    fantrax_positions = []
    unmatched = []
    fuzzy_matches = []

    # Pre-compute normalized names list for fuzzy matching (name only, no team)
    # Extract names from keys (format "name|TEAM")
    all_keys = list(fantrax_lookup.keys())
    all_norm_names = [k.split("|")[0] for k in all_keys]

    def _extract_id(entry) -> Optional[str]:
        """Extract player_id from a lookup entry (dict or legacy str)."""
        if isinstance(entry, dict):
            return entry.get("player_id")
        return entry  # legacy string format

    def _extract_positions(entry) -> str:
        """Extract positions from a lookup entry (dict or legacy str)."""
        if isinstance(entry, dict):
            return entry.get("positions", "")
        return ""

    for _, row in fg_df.iterrows():
        player_name = str(row["name"])
        fg_team = str(row.get("team", ""))

        # Normalize the FanGraphs team abbrev to the Fantrax namespace
        fantrax_team = normalize_team(fg_team, to="fantrax")
        norm_name = normalize_name(player_name)

        # --- Step 1: Exact match ---
        key_fg_team = f"{norm_name}|{fg_team.upper()}"
        key_fantrax_team = f"{norm_name}|{fantrax_team.upper()}"

        entry = (
            fantrax_lookup.get(key_fg_team)
            or fantrax_lookup.get(key_fantrax_team)
        )
        fantrax_id = _extract_id(entry) if entry else None
        pos = _extract_positions(entry) if entry else ""

        if fantrax_id is None:
            # --- Step 2: Fuzzy match on name ---
            close = difflib.get_close_matches(
                norm_name, all_norm_names, n=5, cutoff=threshold
            )

            if close:
                # Among candidates, prefer the one whose team matches
                best_entry = None
                best_score = 0.0

                for candidate_name in close:
                    candidate_keys = [
                        k for k in all_keys if k.split("|")[0] == candidate_name
                    ]
                    for ck in candidate_keys:
                        cand_team = ck.split("|")[1]
                        score = difflib.SequenceMatcher(None, norm_name, candidate_name).ratio()
                        if cand_team in (fg_team.upper(), fantrax_team.upper()):
                            score += 0.05
                        if score > best_score:
                            best_score = score
                            best_entry = fantrax_lookup.get(ck)

                if best_entry:
                    fantrax_id = _extract_id(best_entry)
                    pos = _extract_positions(best_entry)
                    fuzzy_matches.append(
                        f"  FUZZY: '{player_name}' ({fg_team}) "
                        f"-> Fantrax ID {fantrax_id} (score={best_score:.3f})"
                    )

        if fantrax_id is None:
            unmatched.append(f"  UNMATCHED: '{player_name}' ({fg_team})")

        fantrax_ids.append(fantrax_id)
        fantrax_positions.append(pos)

    fg_df["fantrax_id"] = fantrax_ids
    fg_df["fantrax_positions"] = fantrax_positions

    # Log results for manual review
    if fuzzy_matches:
        logger.info("Fuzzy player name matches (review for accuracy):")
        for m in fuzzy_matches:
            logger.info(m)

    if unmatched:
        logger.info("Unmatched FanGraphs players (no Fantrax ID found):")
        for u in unmatched:
            logger.info(u)

    logger.info(
        f"Player matching: {len(fg_df)} FG players, "
        f"{len(fuzzy_matches)} fuzzy, {len(unmatched)} unmatched"
    )

    return fg_df


# ---------------------------------------------------------------------------
# Ohtani identification
# ---------------------------------------------------------------------------


def identify_ohtani(
    batters_df: pd.DataFrame,
    pitchers_df: pd.DataFrame,
) -> tuple:
    """
    Locate Shohei Ohtani's rows in both projection DataFrames.

    Ohtani appears in both batter and pitcher projections on FanGraphs.
    He must be merged into one unified player record in the Z-score engine.

    Parameters
    ----------
    batters_df : pd.DataFrame
        Must have a 'name' column.
    pitchers_df : pd.DataFrame
        Must have a 'name' column.

    Returns
    -------
    tuple[pd.Series, pd.Series]
        (ohtani_bat_row, ohtani_pit_row) as pandas Series.
        Either may be None if not found (logged as a warning).
    """
    target = normalize_name("Shohei Ohtani")

    bat_row = None
    pit_row = None

    bat_match = batters_df[batters_df["name"].apply(normalize_name) == target]
    if not bat_match.empty:
        bat_row = bat_match.iloc[0]
    else:
        logger.warning("Ohtani not found in batter projections.")

    pit_match = pitchers_df[pitchers_df["name"].apply(normalize_name) == target]
    if not pit_match.empty:
        pit_row = pit_match.iloc[0]
    else:
        logger.warning("Ohtani not found in pitcher projections.")

    return bat_row, pit_row
