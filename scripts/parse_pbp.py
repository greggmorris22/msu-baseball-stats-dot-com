"""
parse_pbp.py — Turn cached NCAA play-by-play text into structured per-PA
records for the MSU Splits Selector page.

Input:
    data/scrape-cache.json        — raw scraped games incl. play_by_play
    data/roster-cache.json        — pitcher-name -> throws map per team
    public/data/hitting-stats-2026.json  — for validation (optional)

Output:
    public/data/pbp-events-2026.json     — flat array of structured records

Each output record represents ONE Mississippi State plate appearance (or a
non-PA event we still want to keep for context, like a stolen base). Fields
for a real PA include:

    - contestId, date, opponent, isSEC
    - msu_is_home, inning, half, outs_before, score_msu_before, score_opp_before
    - batter (last-name string, matches NCAA PBP convention)
    - pitcher (opposing pitcher name, best effort), pitcher_hand ("R"/"L"/"S"/null)
    - balls, strikes (count BEFORE the PA-ending pitch, from "(X-Y SEQ)")
    - pitches_seen (length of the "SEQ" string)
    - bases_before: [bool, bool, bool] (1B, 2B, 3B)
    - is_pa: bool (true for real plate appearances)
    - outcome: string label like "single", "double", "walk", "k_swinging", etc.
    - flags: booleans H, 1B, 2B, 3B, HR, BB, IBB, HBP, K, KL, KS, SAC, SF,
             ROE, FC, GO, FO, LO, PO, IFF
    - counting: {AB, PA, TB, OB} (integer contributions for slash-line math)
    - raw_text: original event string

Usage:
    .venv/Scripts/python.exe scripts/parse_pbp.py
    .venv/Scripts/python.exe scripts/parse_pbp.py --validate    (prints diff
        vs scraped NCAA hitting totals)
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path


# ===========================================================================
# Paths
# ===========================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR   = SCRIPT_DIR.parent
CACHE_PATH        = ROOT_DIR / "data" / "scrape-cache.json"
ROSTER_CACHE_PATH = ROOT_DIR / "data" / "roster-cache.json"
LI_TABLE_PATH     = ROOT_DIR / "data" / "leverage-index.json"
HITTING_STATS_PATH  = ROOT_DIR / "public" / "data" / "hitting-stats-2026.json"
PITCHING_STATS_PATH = ROOT_DIR / "public" / "data" / "pitching-stats-2026.json"
OUTPUT_PATH       = ROOT_DIR / "public" / "data" / "pbp-events-2026.json"

TEAM_NAME = "Mississippi St."

# Tango/InsideTheBook leverage bucket cutoffs. Used for the Low/Med/High
# filter on the splits page. A batter_pa or pitcher_pa record's leverage
# value is compared against these to assign a category string.
LI_LOW_MAX  = 0.85   # Low  : LI < 0.85
LI_HIGH_MIN = 2.0    # High : LI > 2.0


# ===========================================================================
# Name normalisation helpers
# ===========================================================================
#
# NCAA data uses inconsistent name formats across pages:
#   - Rosters:         "Jon Smith"        or  "Smith, Jon"
#   - Box scores:      "Jon Smith"
#   - PBP plays:       "Smith"            (last-name only, most common)
#   - PBP subs:        "Smith,Jon"        (last,first, no space)
#
# For pitcher attribution we only need the last name, so all normalisers
# collapse to a lowercase last-name token.


_NAME_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}


def last_name_from(raw):
    """
    Extract a lowercase last-name token from a name in any NCAA format.

    Handles:
      - "Jon Smith"           -> "smith"
      - "Smith, Jon"          -> "smith"
      - "Smith,Jon"           -> "smith"
      - "Smith"               -> "smith"
      - "J.T. Smith"          -> "smith"
      - "Valincius, T."       -> "valincius"
      - "Jevarra Martin Jr."  -> "martin"   (suffix stripped)
      - "Bob Doe III"         -> "doe"      (suffix stripped)
      - ""                    -> ""

    Args:
        raw: Name string in any format.

    Returns:
        Lowercase last-name string, or "" if input is empty.
    """
    if not raw:
        return ""
    raw = raw.strip()

    # "Last, First" or "Last,First" -> take the part before the comma.
    # Also strip glued-on suffixes like "BillingsleyJr,Chris" which NCAA
    # occasionally writes without a separator. We do the suffix stripping
    # BEFORE lowercasing so "BillingsleyJr" -> "Billingsley" via a case-
    # sensitive slice, which is safer than regex against arbitrary names.
    if "," in raw:
        head = raw.split(",", 1)[0].strip()
        for suf in ("Jr.", "Jr", "Sr.", "Sr", "III", "II", "IV", "V"):
            if head.endswith(suf) and len(head) > len(suf) + 1:
                head = head[: -len(suf)]
                break
        return head.strip().lower()

    # "First Last [Jr.]" -> take the last whitespace-separated token,
    # skipping generational suffixes so "Jevarra Martin Jr." becomes
    # "martin" rather than "jr.".
    tokens = raw.split()
    while tokens and tokens[-1].strip().lower() in _NAME_SUFFIXES:
        tokens.pop()
    if not tokens:
        return ""
    return tokens[-1].strip().lower()


def build_hand_index_from_roster(team_entry, field="throws"):
    """
    Build a handedness lookup structure from a roster cache team entry.

    The roster cache supports two formats so we can keep working with both
    legacy and current data on disk:
      - Legacy v1: players: { "Jon Smith": "R" }                  (throws only)
      - Current v2: players: { "Jon Smith": {"throws": "R", "bats": "L"} }

    Args:
        team_entry: One team dict from the roster cache, or None.
        field:      Which handedness column to extract — "throws" (default,
                    used for pitcher_hand resolution) or "bats" (used for
                    bat_side resolution on opponent batters). When the cache
                    is in legacy format, only "throws" is supported; "bats"
                    returns an empty index.

    Returns:
        Dict with "last_name_map" (last-name -> hand) and "players"
        (list of (lowercase full name, lowercase last name, hand) tuples
        for prefix-matching fallback). Empty dict if nothing usable.
    """
    if not team_entry:
        return {}
    # Accept both "players" (new) and "pitchers" (legacy) keys so older
    # roster caches keep working without a rescrape.
    players = team_entry.get("players") or team_entry.get("pitchers") or {}
    ln_map = {}
    collisions = set()
    player_list = []
    for full_name, raw in players.items():
        ln = last_name_from(full_name)
        if not ln:
            continue

        # Pull the requested field out of either the legacy string format
        # (which only carries throws) or the new {"throws":..,"bats":..} dict.
        if isinstance(raw, dict):
            hand = raw.get(field, "") or ""
        else:
            hand = raw if field == "throws" else ""

        if not hand:
            continue

        if ln in ln_map and ln_map[ln] != hand:
            collisions.add(ln)
        ln_map[ln] = hand
        player_list.append((full_name.lower(), ln, hand))
    for ln in collisions:
        ln_map[ln] = ""  # ambiguous -> blank, not a guess
    return {
        "last_name_map": ln_map,
        "players":       player_list,
    }


# ===========================================================================
# Game state tracking helpers
# ===========================================================================
#
# Think of these as the glue between raw PBP text and the nine pieces of
# context a splits page needs: who's batting, who's pitching, outs, bases,
# score differential, count, pitch sequence length, RBI, and leverage index.
#
# The walker in walk_game() below owns the state; these helpers are pure
# functions that either read from or update a simple `bases` dict keyed by
# runner last-name. Using a dict (not an array of booleans) lets us track
# WHICH runner is on which base so sub-event text like
#   "Smith advanced to third"
# can find Smith and move him without guessing. At the moment of a PA we
# snapshot the occupied bases into a 3-char string like "1_3" for the
# leverage lookup and the splits filter.


def _bases_to_key(bases):
    """
    Convert a `{runner_name: base_num}` dict into a 3-char LI table key.

    Examples:
        {}                                  -> "___"
        {"smith": 1}                        -> "1__"
        {"smith": 1, "jones": 3}            -> "1_3"
        {"smith": 1, "jones": 2, "doe": 3}  -> "123"

    Multiple runners on the same base are a bug upstream — we defensively
    keep the last writer and continue.
    """
    slots = ["_", "_", "_"]
    for _runner, b in bases.items():
        if b in (1, 2, 3):
            slots[b - 1] = str(b)
    return "".join(slots)


def _count_state(balls, strikes):
    """
    Classify a pitch count into a sabermetric count state.

    Returns a tuple (primary, is_two_strike) where primary is one of:
        "ahead"  — pitcher ahead:   0-1, 0-2, 1-2
        "even"   — neutral:         0-0, 1-1, 2-2
        "behind" — pitcher behind:  1-0, 2-0, 2-1, 3-0, 3-1, 3-2

    The two-strike flag is set whenever strikes == 2, which overlaps with
    ahead/even/behind on purpose (a 3-2 count is BOTH "behind" and
    "two-strike", and both filters should show it).

    NCAA PBP reports the count BEFORE the PA-ending pitch, so a "3-2"
    count means the final pitch was thrown with a full count. That's the
    sense we want for filter categorisation.

    Args:
        balls: 0-3 or None.
        strikes: 0-2 or None.

    Returns:
        (state_label, two_strike_bool). Returns ("even", False) if the
        count is missing so the record still ends up in an identifiable
        bucket.
    """
    if balls is None or strikes is None:
        return ("even", False)

    two_strike = (strikes == 2)

    # Ahead / even / behind classification. "Even" is exactly the diagonal
    # 0-0, 1-1, 2-2 (and conventionally 3-2 is "behind" even though counts
    # are equal numerically, because the pitcher must come in).
    if balls == strikes and balls < 3:
        return ("even", two_strike)
    if balls < strikes:
        return ("ahead", two_strike)
    # balls > strikes, or 3-2
    return ("behind", two_strike)


def _parse_rbi(text):
    """
    Extract the RBI count from a PA outcome string.

    NCAA formats it a few different ways:
        "singled to left, RBI"          -> 1
        "singled to left, 2 RBI"        -> 2
        "homered to right, 3RBI"        -> 3  (no space)
        "walked, RBI"                    -> 1
        "struck out swinging"            -> 0

    Args:
        text: PA event text string.

    Returns:
        Integer 0, 1, 2, 3, or 4.
    """
    if not text:
        return 0
    # "N RBI" or "NRBI" where N is 1-4
    m = re.search(r"(\d)\s*RBI\b", text)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return 0
    # Bare "RBI" with no digit -> 1
    if re.search(r"\bRBI\b", text):
        return 1
    return 0


def _apply_pa_to_bases(bases, batter_key, outcome):
    """
    Place the batter on the correct base after their PA, mutating the
    `bases` dict in place.

    This is only the BATTER's movement from the PA outcome alone. Runner
    movements described in sub_events are handled separately by
    `_apply_subevents_to_bases`. The two together describe the complete
    base-state change on a play.

    Handled outcomes (batter lands somewhere):
        single, walk, ibb, hbp, roe, fc, ci -> 1B
        double                              -> 2B
        triple                              -> 3B
        home_run                            -> scores (no base)

    Everything else retires the batter and leaves `bases` untouched.

    Args:
        bases: {runner_last_name: base_num} dict to mutate.
        batter_key: Lowercase last-name of the batter.
        outcome: Outcome label string (from classify_event).
    """
    if outcome in ("single", "walk", "ibb", "hbp", "roe", "fc", "ci"):
        bases[batter_key] = 1
    elif outcome == "double":
        bases[batter_key] = 2
    elif outcome == "triple":
        bases[batter_key] = 3
    # home_run, outs, strikeouts, sacrifices: no base assignment


# Sub-event patterns. The PBP text for sub_events is a short list of
# human-readable sentences: "<Name> scored", "<Name> scored from second",
# "<Name> advanced to third", "<Name> stole second", "<Name> out at second",
# "<Name> picked off", "<Name> caught stealing". We apply each to the bases
# dict in the order they appear.
#
# Important: a runner's name in a sub_event might be "Smith" OR
# "Smith,Bryce" (last,first). We normalise via last_name_from before
# touching the dict, so the key is the same as what _apply_pa_to_bases
# uses.

_SUB_SCORED_RE   = re.compile(r"^(.+?)\s+scored\b", re.IGNORECASE)
_SUB_ADV_RE      = re.compile(r"^(.+?)\s+advanced to\s+(first|second|third|home)\b", re.IGNORECASE)
_SUB_STOLE_RE    = re.compile(r"^(.+?)\s+stole\s+(second|third|home)\b", re.IGNORECASE)
_SUB_OUT_AT_RE   = re.compile(r"^(.+?)\s+out at\s+(first|second|third|home|\dB)\b", re.IGNORECASE)
_SUB_PICKED_RE   = re.compile(r"^(.+?)\s+picked off\b", re.IGNORECASE)
_SUB_CAUGHT_RE   = re.compile(r"^(.+?)\s+caught stealing\b", re.IGNORECASE)

_BASE_NAME_TO_NUM = {
    "first": 1, "1b": 1,
    "second": 2, "2b": 2,
    "third": 3, "3b": 3,
    "home": 0, "hp": 0,  # 0 = scored (off bases)
}


def _apply_subevents_to_bases(bases, sub_events):
    """
    Walk a PA's sub_events list and update `bases` accordingly.

    Returns a tuple (runs_scored, outs_on_bases) so the walker can update
    its running totals. Note that runs_scored here only counts runners
    other than the batter — a home run's batter run is counted separately
    by the walker from the PA outcome.

    Rules applied in order:
        "X scored[...]"                 -> remove X from bases, runs++
        "X advanced to home"            -> remove X, runs++
        "X advanced to second/third"    -> set X's base
        "X stole second/third/home"     -> advance X (home = remove + run)
        "X out at ..."                  -> remove X, outs++
        "X picked off"                  -> remove X, outs++
        "X caught stealing"             -> remove X, outs++

    Sub-events may describe runners NCAA does not otherwise show on base
    (if our earlier parsing missed a runner). In those cases we silently
    skip rather than invent a phantom runner.

    Args:
        bases: {last_name: base_num} dict to mutate.
        sub_events: list of sub-event strings from the PBP cache.

    Returns:
        (runs_scored, outs_on_bases): integer counters.
    """
    runs = 0
    outs = 0
    for sub in sub_events or []:
        s = sub.strip().rstrip(".")
        if not s:
            continue

        # "X scored"
        m = _SUB_SCORED_RE.match(s)
        if m:
            key = last_name_from(m.group(1))
            if key in bases:
                del bases[key]
            runs += 1
            continue

        # "X advanced to base"
        m = _SUB_ADV_RE.match(s)
        if m:
            key = last_name_from(m.group(1))
            base_num = _BASE_NAME_TO_NUM.get(m.group(2).lower(), 0)
            if base_num == 0:
                if key in bases:
                    del bases[key]
                runs += 1
            else:
                bases[key] = base_num
            continue

        # "X stole base"
        m = _SUB_STOLE_RE.match(s)
        if m:
            key = last_name_from(m.group(1))
            base_num = _BASE_NAME_TO_NUM.get(m.group(2).lower(), 0)
            if base_num == 0:
                if key in bases:
                    del bases[key]
                runs += 1
            else:
                bases[key] = base_num
            continue

        # "X out at ..."
        m = _SUB_OUT_AT_RE.match(s)
        if m:
            key = last_name_from(m.group(1))
            if key in bases:
                del bases[key]
            outs += 1
            continue

        # "X picked off"
        m = _SUB_PICKED_RE.match(s)
        if m:
            key = last_name_from(m.group(1))
            if key in bases:
                del bases[key]
            outs += 1
            continue

        # "X caught stealing"
        m = _SUB_CAUGHT_RE.match(s)
        if m:
            key = last_name_from(m.group(1))
            if key in bases:
                del bases[key]
            outs += 1
            continue

    return runs, outs


def _lookup_leverage(li_table, inning, half, bases_key, outs, score_diff):
    """
    Look up the leverage index for a game state.

    The LI table is keyed by "{inning}_{half}" -> base_key -> outs_str ->
    score_diff_str. Score differential is from the batting team's
    perspective and clamped to [-4, +4] — anything outside is an extreme
    blowout where LI is effectively 0.

    Args:
        li_table: Loaded leverage-index.json dict.
        inning:   1+ integer (extras clone inning 9).
        half:     "top" or "bottom".
        bases_key: 3-char string from _bases_to_key.
        outs:     0, 1, or 2.
        score_diff: integer (batting team runs - fielding team runs).

    Returns:
        Float LI value, or None if no lookup could be made. Returning None
        lets the walker tag the PA for later diagnosis.
    """
    if not li_table:
        return None
    if inning is None or half is None:
        return None

    # Extra-inning fallback: anything beyond 9 maps to the inning 9 table
    # (which we cloned at build time anyway).
    inning_key = str(inning) if inning <= 15 else "9"
    section_key = f"{inning_key}_{half}"
    section = li_table.get(section_key)
    if not section:
        return None

    by_outs = section.get(bases_key)
    if not by_outs:
        return None

    by_diff = by_outs.get(str(outs))
    if not by_diff:
        return None

    # Clamp score differential to [-4, +4].
    if score_diff is None:
        score_diff = 0
    clamped = max(-4, min(4, int(score_diff)))
    if clamped > 0:
        diff_key = f"+{clamped}"
    elif clamped < 0:
        diff_key = str(clamped)  # "-3"
    else:
        diff_key = "0"

    return by_diff.get(diff_key)


def _leverage_bucket(li_value):
    """
    Return the Low/Med/High bucket label for a leverage value.

    Cutoffs from Tango/InsideTheBook:
        Low    : LI < 0.85
        Medium : 0.85 <= LI <= 2.0
        High   : LI > 2.0

    Args:
        li_value: Float, or None.

    Returns:
        "low", "medium", "high", or None.
    """
    if li_value is None:
        return None
    if li_value < LI_LOW_MAX:
        return "low"
    if li_value > LI_HIGH_MIN:
        return "high"
    return "medium"


def _parse_score_field(score_str):
    """
    Parse NCAA's "away-home" score string into (away, home) integers.

    Examples:
        "0-0"  -> (0, 0)
        "5-6"  -> (5, 6)
        ""     -> (0, 0)
        None   -> (0, 0)

    Args:
        score_str: String like "3-2" or None.

    Returns:
        Tuple of two ints, defaulting to (0, 0) on parse failure.
    """
    if not score_str:
        return (0, 0)
    m = re.match(r"^(\d+)\s*-\s*(\d+)$", score_str.strip())
    if not m:
        return (0, 0)
    try:
        return (int(m.group(1)), int(m.group(2)))
    except ValueError:
        return (0, 0)


# ===========================================================================
# Event classification regexes
# ===========================================================================
#
# PBP events we care about fall into a handful of buckets. Order matters in
# the classifier below: we test the most specific patterns first so that e.g.
# "reached on a fielding error" isn't mis-classified as just "reached".

# Pitching changes
#
# NCAA PBP has surprisingly many variants:
#   "Reese to p for Martinez."
#   "Crotchfelt, to p for Dean, Blake."   (stray comma, multi-token names)
#   "Tapper, Broc to p for Crotchfelt,."
#   "Dylan Bryan to p for Brady Blum."    (first+last names)
#   "ASU pitching change: Butler,Josh replaces Carlon,Cole."
#
# FMT1 matches the first four: "<name> to p for <name>." with any mix of
# commas, spaces, apostrophes and hyphens inside the name groups.
PITCH_CHANGE_FMT1 = re.compile(
    r"^([A-Z][\w.',\-]*(?:\s+[A-Z]?[\w.',\-]+)*?)\s+to p for\s+"
    r"([A-Z][\w.',\-]*(?:\s+[A-Z]?[\w.',\-]+)*?)\.?\s*$"
)
PITCH_CHANGE_FMT2 = re.compile(
    r"pitching change:\s*([A-Z][\w.'\-,\s]*?)\s+replaces\s+([A-Z][\w.'\-,\s]*?)\.",
    re.IGNORECASE,
)

# Defensive position changes and new fielder announcements:
#   "Oriach to dh."
#   "Boroff, Jabe to dh."
#   "Caston, Gavi to rf for Hawk, Keatan."
#   "Johnson, Cha to 1b for Harris, Coop."
# The position codes cover every fielding spot except p (which is handled
# by the pitching-change regex above, since "to p for X" is a pitching
# change). DH is included because the DH position is tracked in PBP.
DEF_SUB_RE = re.compile(
    r"^[A-Z][\w.',\-\s]+?\s+to\s+(?:c|1b|2b|3b|ss|lf|cf|rf|dh)(?:\s+for\s+|\.)",
    re.IGNORECASE,
)

# Lineup subs / position changes / mound visits / replay — all non-PA
NON_PA_PATTERNS = [
    re.compile(r"\bpinch (?:hit|ran|hitting|running) for\b", re.IGNORECASE),
    re.compile(r"\bsubstitution\b:", re.IGNORECASE),
    re.compile(r"\bposition change\b", re.IGNORECASE),
    re.compile(r"\bdefensive substitution\b", re.IGNORECASE),
    re.compile(r"\bmound visit\b", re.IGNORECASE),
    re.compile(r"\bchallenges the call\b", re.IGNORECASE),
    re.compile(r"\bis challenging the call\b", re.IGNORECASE),
    re.compile(r"^previous pa:", re.IGNORECASE),
    re.compile(r"^dropped foul ball", re.IGNORECASE),
    # Offensive timeouts. We match "timeout", the occasional "itimeout", and
    # the "imeout" (missing leading 't') typo that appears in some games.
    # Also accept "0ffensive" (zero-for-O typo we've seen in LIP games).
    re.compile(r"[0o]ffensive\s+(?:timeout|itimeout|imeout)", re.IGNORECASE),
    # Replay review announcements in any form.
    re.compile(r"^review:", re.IGNORECASE),
    # Crew chief / umpire replay review notes (e.g. "Crew chief review of
    # boundary call of HR or no HR."). These are parenthetical game-event
    # notes that do not represent a PA by themselves.
    re.compile(r"^crew chief review\b", re.IGNORECASE),
    # Pitch clock violations that are attached to the PREVIOUS batter —
    # e.g. "Bauer had 2 pitch clock violations on last batter." These are
    # pitcher notes, not PAs.
    re.compile(r"\bpitch clock violation", re.IGNORECASE),
    # Malformed lines with nothing but a stray "/ for X" phrase.
    re.compile(r"^/\s+for\b", re.IGNORECASE),
    # Player ejections: "Sullivan (MSU) is ejected."
    re.compile(r"\bis ejected\b", re.IGNORECASE),
    # Replay challenges phrased as "FC to X by Y is being challenged ..."
    re.compile(r"\bis being challenged\b", re.IGNORECASE),
    # Compact informal FC line without a pitch count, e.g.
    #     "Woodson FC to SS, Frei out 64"
    # These are placeholder lines that the NCAA PBP emits BEFORE a replay
    # review; the formal "reached on a fielder's choice to shortstop (X-Y)"
    # line follows, so we must NOT count both. Gated on the absence of a
    # pitch count in parens, which distinguishes it from the real PA line.
    re.compile(r"^[A-Z]\w*\s+FC to [A-Z]+,\s+\w+\s+out\s+\d+\s*$"),
]

# Running-play events (non-PA state changes)
STOLEN_BASE_RE      = re.compile(r"\bstole\s+(second|third|home)\b", re.IGNORECASE)
CAUGHT_STEALING_RE  = re.compile(r"\bcaught stealing\b", re.IGNORECASE)
PICKED_OFF_RE       = re.compile(r"\bpicked off\b", re.IGNORECASE)
WILD_PITCH_RE       = re.compile(r"\bwild pitch\b", re.IGNORECASE)
PASSED_BALL_RE      = re.compile(r"\bpassed ball\b", re.IGNORECASE)
# Name may include commas and a first-name initial (e.g. "Nunnallee,James").
ADVANCED_ONLY_RE    = re.compile(r"^[A-Z][\w.',\-\s]+?\s+(?:advanced|scored)\b")

# PA outcome regexes. Ordered from most-specific to least-specific.
# IMPORTANT: sac_fly and sac_bunt are tested BEFORE the regular fly/ground
# patterns so that "flied out to rf, SF, RBI" is classified as a sac fly
# (AB=0) rather than a plain fly out (AB=1). Same for "grounded out..., SAC, bunt".
OUTCOME_PATTERNS = [
    # Strikeouts
    ("k_looking",  re.compile(r"\bstruck out looking\b", re.IGNORECASE)),
    ("k_swinging", re.compile(r"\bstruck out swinging\b", re.IGNORECASE)),
    ("k_other",    re.compile(r"\bstruck out\b", re.IGNORECASE)),

    # Walks
    ("ibb",  re.compile(r"\bintentionally walked\b", re.IGNORECASE)),
    ("walk", re.compile(r"\bwalked\b", re.IGNORECASE)),

    # Hit by pitch
    ("hbp",  re.compile(r"\bhit by pitch\b", re.IGNORECASE)),

    # Hits (order: HR first, then 3B, 2B, 1B)
    ("home_run", re.compile(r"\bhomered\b|\bhome run\b", re.IGNORECASE)),
    ("triple",   re.compile(r"\btripled\b", re.IGNORECASE)),
    ("double",   re.compile(r"\bdoubled\b", re.IGNORECASE)),
    ("single",   re.compile(r"\bsingled\b", re.IGNORECASE)),

    # Sacrifice plays — matched BEFORE flied/grounded below so "flied out,
    # SF, RBI" turns into AB=0 sac fly, not AB=1 flyout.
    ("sac_fly",  re.compile(
        r"\bsacrifice fly\b|\bsac(?:rificed)?\s+fly\b|,\s*SF[,.]|\bSF\s*,",
        re.IGNORECASE,
    )),
    ("sac_bunt", re.compile(
        r"\bsacrifice bunt\b|\bsac(?:rificed)?\s+bunt\b|"
        r",\s*SAC\s*,\s*bunt\b|\b(?:out|safe) on a sacrifice\b",
        re.IGNORECASE,
    )),

    # Catcher's interference — PA but NOT an AB. Must come BEFORE the
    # generic "reached" catch-all, otherwise it gets bucketed into ROE
    # and charged an at-bat. Rule: the batter is awarded first base as
    # a PA on defensive interference; NCAA does not charge an AB.
    ("ci", re.compile(
        r"\breached on (?:a |an )?(?:catcher's )?interference\b",
        re.IGNORECASE,
    )),

    # Reached on error / fielder's choice
    ("roe", re.compile(
        r"\breached on (?:a |an )?(?:fielding |throwing |catcher's )?error\b",
        re.IGNORECASE,
    )),
    ("fc", re.compile(
        r"\bfielder's choice\b|\bFC\s+to\b",
        re.IGNORECASE,
    )),

    # Double plays — matched before grounded/lined/flied so the DP contributes
    # a single out for the batter regardless of fielded position.
    ("gidp", re.compile(
        r"\bgrounded into double play\b|\bhit into double play\b|"
        r"\blined into double play\b|\bflew into double play\b",
        re.IGNORECASE,
    )),

    # In-play outs (by trajectory)
    ("infield_fly", re.compile(r"\binfield fly\b", re.IGNORECASE)),
    ("popped",      re.compile(r"\bpopped\b", re.IGNORECASE)),
    ("fouled_out",  re.compile(r"\bfouled out\b", re.IGNORECASE)),
    ("flied_out",   re.compile(r"\b(?:flied|flew)\b", re.IGNORECASE)),
    ("lined_out",   re.compile(r"\blined\b", re.IGNORECASE)),
    ("grounded_out", re.compile(r"\bgrounded\b", re.IGNORECASE)),

    # Generic "out at first" PA — e.g. "Nunnallee out at first p to ss to 1b
    # (3-2 KBBFB)." These are real at-bats where NCAA just elided the
    # verb (grounded/lined/bunted). We classify them as ground outs since
    # the fielding sequence is typically an infielder-to-first play. This
    # is gated on the outcome pattern being tested AFTER the more specific
    # patterns above, so "flied out to rf" or "struck out swinging, out at
    # first c to 1b" still take precedence.
    ("out_at_first", re.compile(r"\bout at first\b", re.IGNORECASE)),

    # Catch-all "reached" (treat as ROE unless something more specific matched)
    ("reached", re.compile(r"\breached\b", re.IGNORECASE)),
]

# Count pattern:  "(3-2 BBBKF)"  or  "(0-0)"  or  "(1-2 KBS)"
COUNT_RE = re.compile(r"\((\d+)-(\d+)(?:\s+([A-Z]+))?\)")

# Batter name at the start of the line. Accepts these forms:
#   "Woodson"          (single last name)
#   "Nunnallee,James"  (Last,First with no space)
#   "Valincius, T."    (Last, First with space after comma, with trailing dot)
#   "Azpilcueta,"      (Last, — trailing comma with no first-name token)
# Anchored by a mandatory whitespace after the name, which means we stop
# before abbreviations like "FC" in "Woodson FC to SS..." (FC is NOT part
# of the name, it's an outcome token). The trailing `,?` lets us capture
# the "Azpilcueta, struck out swinging." case where NCAA wrote the name
# with a bare trailing comma.
# NCAA's PBP shows player names in (at least) three formats across teams:
#   1. "Lastname verb..."          — single-token surname
#   2. "Lastname, F. verb..."      — surname with trailing first initial
#   3. "F. Lastname verb..."       — first initial PRECEDING surname
#
# The optional `(?:[A-Z]\.\s+)?` non-capturing prefix lets us SKIP a leading
# initial-and-period in case 3 so that group 1 always lands on the actual
# surname, not the first initial. The prefix only matches a single capital
# letter immediately followed by a period and whitespace, so plain words
# like "Frei" or "Allen" don't accidentally trigger it.
BATTER_NAME_RE = re.compile(
    # Captures the batter name from the start of a PBP event line.
    # Handles four NCAA name formats:
    #   1. "Smith singled..."           -> "Smith"
    #   2. "Smith, J. singled..."       -> "Smith, J."
    #   3. "J. Smith singled..."        -> "Smith"  (leading initial skipped)
    #   4. "Jordan Smith singled..."    -> "Jordan Smith"
    #      (multi-word; last_name_from() extracts "smith" downstream)
    #
    # The key: additional capitalized words are consumed by the trailing
    # (?:\s+[A-Z][\w.'\-]*)* group, which stops at the first lowercase
    # token (the outcome verb like "singled", "walked", "grounded").
    r"^(?:[A-Z]\.\s+)?([A-Z][\w.'\-]*(?:,\s*[A-Z][\w.'\-]*\.?)?(?:\s+[A-Z][\w.'\-]*)*),?\s+"
)


# ===========================================================================
# Outcome metadata
# ===========================================================================
#
# For each outcome type, define the stat contributions. This drives the
# aggregation and slash-line math:
#   AB  = at bat (excludes BB, HBP, SAC)
#   PA  = plate appearance (everything except pure running events)
#   H   = hit
#   TB  = total bases
#   OB  = on-base for OBP numerator (H, BB, HBP)
#   BB, HBP, K = individual counters

OUTCOME_META = {
    #                          AB PA  H TB OB BB HBP K   Notes
    "single":      dict(AB=1, PA=1, H=1, TB=1, OB=1),
    "double":      dict(AB=1, PA=1, H=1, TB=2, OB=1),
    "triple":      dict(AB=1, PA=1, H=1, TB=3, OB=1),
    "home_run":    dict(AB=1, PA=1, H=1, TB=4, OB=1),
    "walk":        dict(AB=0, PA=1, H=0, TB=0, OB=1, BB=1),
    "ibb":         dict(AB=0, PA=1, H=0, TB=0, OB=1, BB=1, IBB=1),
    "hbp":         dict(AB=0, PA=1, H=0, TB=0, OB=1, HBP=1),
    "k_looking":   dict(AB=1, PA=1, K=1, KL=1),
    "k_swinging":  dict(AB=1, PA=1, K=1, KS=1),
    "k_other":     dict(AB=1, PA=1, K=1),
    "sac_fly":     dict(AB=0, PA=1, SF=1),     # counts in OBP denominator
    # Sac bunts and catcher's interference are NOT counted in the NCAA PA
    # column on either the batter or pitcher side (PA = AB + BB + HBP + SF).
    # We still emit full records for them so the splits page can filter
    # by state, but PA=0 keeps the aggregate counter matching NCAA.
    "sac_bunt":    dict(AB=0, PA=0, SAC=1),
    "ci":          dict(AB=0, PA=0, OB=1, CI=1),
    "roe":         dict(AB=1, PA=1, ROE=1),    # still an AB, does NOT count as OB
    "fc":          dict(AB=1, PA=1, FC=1),
    "gidp":        dict(AB=1, PA=1, GIDP=1),   # ground/hit/line into double play
    "grounded_out": dict(AB=1, PA=1, GO=1),
    "out_at_first": dict(AB=1, PA=1, GO=1),   # batter retired at first
    "flied_out":    dict(AB=1, PA=1, FO=1),
    "lined_out":    dict(AB=1, PA=1, LO=1),
    "popped":       dict(AB=1, PA=1, PO=1),
    "infield_fly":  dict(AB=1, PA=1, IFF=1),
    "fouled_out":   dict(AB=1, PA=1, FO=1),
    "reached":      dict(AB=1, PA=1, ROE=1),   # fallback bucket
}


# ===========================================================================
# Classifier
# ===========================================================================

def classify_event(text):
    """
    Classify a single PBP event-text line.

    Classification priority is important here. We try the most RELIABLE
    signals first so that an ambiguous event like "Frei grounded out to 2b."
    is correctly read as a PA (ground-out verb present), not a defensive
    sub ("to 2b." matches DEF_SUB_RE).

    Priority order:
      1. Pitching changes    (explicit "to p for" / "pitching change:")
      2. Obvious non-PA text (subs, mound visits, timeouts, reviews, ejections)
      3. Real plate appearance (batter name + outcome verb anywhere in line)
      4. Running-only plays  (SB, CS, tag-outs, advance-only)
      5. Defensive position change (catch-all for "Name to dh." etc.)
      6. Unknown

    Args:
        text: Event text string from the PBP cache.

    Returns:
        A dict describing the classification. Always has a "type" key.
    """
    if not text:
        return {"type": "unknown"}

    # --- Priority 1: Pitching changes ---
    m = PITCH_CHANGE_FMT2.search(text)
    if m:
        return {
            "type":        "pitch_change",
            "new_pitcher": m.group(1).strip(),
            "old_pitcher": m.group(2).strip(),
        }
    m = PITCH_CHANGE_FMT1.match(text)
    if m:
        return {
            "type":        "pitch_change",
            "new_pitcher": m.group(1).strip(),
            "old_pitcher": m.group(2).strip(),
        }

    # --- Priority 2: Non-PA text (subs, mound visits, timeouts, reviews) ---
    for pat in NON_PA_PATTERNS:
        if pat.search(text):
            return {"type": "non_pa"}

    # --- Priority 3: Plate appearance (name + outcome verb) ---
    # Outcomes are the most specific signal, so we try this BEFORE the
    # running-play and def-sub patterns. Without this ordering, a line
    # like "Frei grounded out to 2b." gets mis-classified as a fielding
    # sub because the regex accepts "grounded out" as part of the name.
    name_match = BATTER_NAME_RE.match(text)
    raw_name = name_match.group(1).strip().rstrip(",").strip() if name_match else ""

    # "Runner-pickoff with a tag-out" events look like "Melara out at first
    # p to 1b, picked off." which would otherwise match the `out_at_first`
    # PA outcome. But we can't blanket-gate ALL outcomes on "picked off"
    # being present, because NCAA sometimes emits real PA lines like
    # "Teel struck out swinging, hit into double play c to ss (2-2 FBBFS);
    # Chance out on the play, caught stealing." where the strikeout is
    # the real PA and the CS is incidental. So gate ONLY `out_at_first`
    # against runner-out markers. Real PAs don't use "out at first" text
    # when the runner is picked off.
    is_runner_pickoff = bool(
        re.search(r"\bpicked off\b", text, re.IGNORECASE)
        or re.search(r"\bcaught stealing\b", text, re.IGNORECASE)
    )

    if raw_name:
        for label, pat in OUTCOME_PATTERNS:
            if label == "out_at_first" and is_runner_pickoff:
                continue
            if pat.search(text):
                # Normalise to last-name-only so "Bevis" and "Bevis,Blake"
                # merge into a single batter key. Title-case for display.
                batter = last_name_from(raw_name).title()

                # Count extraction: "(3-2 BBBKF)"
                balls = strikes = pitches_seen = None
                cm = COUNT_RE.search(text)
                if cm:
                    balls        = int(cm.group(1))
                    strikes      = int(cm.group(2))
                    pitches_seen = len(cm.group(3)) if cm.group(3) else 0

                return {
                    "type":         "pa",
                    "batter":       batter,
                    "outcome":      label,
                    "balls":        balls,
                    "strikes":      strikes,
                    "pitches_seen": pitches_seen,
                }

    # --- Priority 4: Running-only plays ---
    if ADVANCED_ONLY_RE.match(text):
        return {"type": "running", "subtype": "advance"}
    if STOLEN_BASE_RE.search(text):
        return {"type": "running", "subtype": "sb"}
    # Runner tagged out on the bases, e.g. "Valincius, V out at second c to 2b."
    if re.match(r"^[A-Z][\w.',\-\s]+?\s+out at\s+", text, re.IGNORECASE):
        return {"type": "running", "subtype": "out"}
    if WILD_PITCH_RE.search(text) or PASSED_BALL_RE.search(text):
        return {"type": "running", "subtype": "wp_pb"}
    # NCAA shorthand for caught stealing / pickoff:
    #   "Hall, Jr. UAB called out 24 CS"
    #   "Waugh UAB is called out 25 CS"
    #   "Jones called out 13 PK"
    # These are runner-out events with the CS / PK token at the end, and
    # are NOT plate appearances.
    if re.search(r"\bcalled out\b.*\b(?:CS|PK)\b", text):
        return {"type": "running", "subtype": "cs_pk"}
    if CAUGHT_STEALING_RE.search(text) or PICKED_OFF_RE.search(text):
        return {"type": "running", "subtype": "cs_pk"}

    # --- Priority 5: Defensive position changes ---
    # Catch-all for "Name to dh.", "Name to 1b for OtherName."
    if DEF_SUB_RE.match(text):
        return {"type": "non_pa"}

    # --- Priority 6: Unknown ---
    batter_guess = last_name_from(raw_name).title() if raw_name else ""
    return {"type": "unknown", "batter": batter_guess, "raw": text}


# ===========================================================================
# Game walker
# ===========================================================================

def walk_game(
    game_entry,
    opp_hand_index,
    msu_hand_index,
    li_table,
    msu_batter_canon=None,
    msu_pitcher_canon=None,
    opp_bat_index=None,
    msu_bat_index=None,
):
    """
    Walk one game's full PBP (both halves of every inning) and emit:
      - batter_pas: one record per MSU plate appearance
      - pitcher_pas: one record per opponent plate appearance (MSU pitching)

    The walker carries a live game-state: current pitcher on each side,
    outs, bases dict, score, and the split-aware context needed for the
    splits filter. For every PA we snapshot that state BEFORE applying
    the PA outcome, so filters like "count state" or "bases before" ask
    "what was the world like when the pitcher released the decisive
    pitch?" — not "after the ball was in play".

    Args:
        game_entry: One cache entry (dict stored under a contest ID).
        opp_hand_index: Throwing-hand index for the opposing team's roster
            (resolves the opposing pitcher's throws — used for MSU batter
            splits vs LHP/RHP).
        msu_hand_index: Throwing-hand index for MSU's own roster (resolves
            the MSU pitcher's throws). Currently empty since we don't
            scrape the MSU roster.
        li_table: Loaded leverage-index.json dict (may be empty).
        msu_batter_canon: {lowercase last name: "Title"} canonical MSU
            batter-name lookup (from hitting-stats JSON). Used to merge
            PBP typos like "Nunnalee" into "Nunnallee".
        msu_pitcher_canon: Same shape as msu_batter_canon but for MSU
            pitchers, so "to p for X" events can be snapped to the canonical
            pitcher name.
        opp_bat_index: Bat-side index for the opposing team's roster
            (resolves the opposing batter's bats — used for the pitcher
            splits page's vs LHB/RHB filter).
        msu_bat_index: Bat-side index for MSU's roster (would resolve the
            MSU batter's bats). Currently unused; reserved for parity.

    Returns:
        (batter_pas, pitcher_pas, diagnostics) tuple of:
          - batter_pas: list of PA-record dicts for MSU's hitting side
          - pitcher_pas: list of PA-record dicts for MSU's pitching side
          - diagnostics: dict of parse stats for validation output
    """
    pbp = game_entry.get("play_by_play", {}) or {}
    innings = pbp.get("innings") or []
    if not innings:
        return [], [], {"innings": 0}

    # Identify which half of each inning each team bats.
    msu_is_home  = TEAM_NAME in (pbp.get("home_team") or "")
    msu_bat_half = "bottom" if msu_is_home else "top"
    opp_bat_half = "top"    if msu_is_home else "bottom"

    # Starting pitcher for each side.
    opp_pitchers = game_entry.get("opponent_pitchers") or []
    current_opp_pitcher = opp_pitchers[0] if opp_pitchers else None

    msu_pitchers = game_entry.get("pitcher_list") or []
    current_msu_pitcher = msu_pitchers[0] if msu_pitchers else None

    batter_pas  = []
    pitcher_pas = []
    diag = {
        "events":            0,
        "pas":               0,
        "msu_pas":           0,
        "opp_pas":           0,
        "unknowns":          0,
        "pitch_changes":     0,
        "running":           0,
        "non_pa":            0,
        "opp_hand_hits":     0,
        "opp_hand_misses":   0,
        "msu_hand_hits":     0,
        "msu_hand_misses":   0,
        "opp_bat_hand_hits":   0,
        "opp_bat_hand_misses": 0,
        "leverage_hits":     0,
        "leverage_misses":   0,
    }

    # Per-game context
    contest_id = game_entry.get("contestId", "")
    date       = game_entry.get("date", "")
    opponent   = game_entry.get("opponent", "")
    is_sec     = bool(game_entry.get("isSEC"))
    opp_team_id = game_entry.get("opponentTeamId")

    # Neutral-site detection: two methods —
    #   1. The scraper appends "@City, ST" to the opponent name for some
    #      neutral venues (e.g. "Arizona St.@Arlington, TX").
    #   2. Explicit contest ID list for neutral games where the opponent
    #      name doesn't carry the "@" marker (e.g. Virginia Tech at the
    #      Globe Life Invitational in Arlington, TX).
    # The Ole Miss neutral game (4/28/2026) will be added here once played.
    NEUTRAL_CONTEST_IDS = {
        "6494009",   # 02/27 vs Arizona St. @ Arlington, TX
        "6494015",   # 02/28 vs Virginia Tech @ Arlington, TX
        "6494020",   # 03/01 vs UCLA @ Arlington, TX
        "6486691",   # 03/10 vs Tulane @ Biloxi, MS
    }
    is_neutral = "@" in opponent or str(contest_id) in NEUTRAL_CONTEST_IDS

    # Running game score tracked as (away, home) integers, mirroring the
    # NCAA "away-home" score string in each event. We update this AFTER
    # each event from the score field so the NEXT event sees the correct
    # pre-play score.
    away_score, home_score = 0, 0

    for inning in innings:
        inn_num = inning.get("inning")

        # Walk BOTH halves in order so the running game state stays
        # coherent across an inning boundary.
        for half_label in ("top", "bottom"):
            half = inning.get(half_label) or {}
            events = half.get("events") or []

            is_msu_batting = (half_label == msu_bat_half)

            # Reset per half-inning. Bases are empty, outs = 0.
            outs   = 0
            bases  = {}

            # Index of the most recent MSU pitcher PA in this half. Used
            # to back-credit running-play outs (caught stealing, pickoffs)
            # to the pitcher who was on the mound when they happened, so
            # IP totals match NCAA. Reset to None at each half-inning.
            last_pitcher_pa_idx_this_half = None

            for ev_idx, event in enumerate(events):
                text = (event.get("text") or "").strip()
                if not text:
                    continue
                diag["events"] += 1

                # ---- Pre-review placeholder detection ----
                # NCAA sometimes emits a shorthand event BEFORE a replay
                # review, which is then replaced by the formal PA line
                # after the review. The placeholder:
                #   - is directly followed by a "Review: ..." event,
                #   - lacks the "(N-N XYZ)" pitch-count that every real PA has.
                # Examples we've seen in MSU 2026 PBP:
                #   "Allen UGA grounds into a 643 GDP"
                #   "Phelps UGA grounds into a FC 6"
                #   "Hall, Jr. UAB called out 24 CS"
                #   "Eisfielder UT grounds out 43"
                # Treat these as non-PA so the real PA line (which follows
                # the Review) is the only event that counts.
                next_text = ""
                if ev_idx + 1 < len(events):
                    next_text = (events[ev_idx + 1].get("text") or "").strip()
                if (
                    next_text.lower().startswith("review:")
                    and not COUNT_RE.search(text)
                ):
                    diag["non_pa"] += 1
                    if event.get("score"):
                        a, h = _parse_score_field(event.get("score"))
                        away_score, home_score = a, h
                    continue

                # Post-review explanatory notes: NCAA occasionally emits a
                # short "VU Waite was hit by ball out of the box on a bunt
                # attempt." style note AFTER the real PA (which already
                # counted). These notes start with a 2-4 letter uppercase
                # team code and have no pitch count.
                if (
                    re.match(r"^[A-Z]{2,4}\s+[A-Z]", text)
                    and not COUNT_RE.search(text)
                ):
                    diag["non_pa"] += 1
                    if event.get("score"):
                        a, h = _parse_score_field(event.get("score"))
                        away_score, home_score = a, h
                    continue

                sub_events     = event.get("sub_events") or []
                ev_score       = event.get("score")
                ev_away, ev_home = _parse_score_field(ev_score)

                cls = classify_event(text)
                etype = cls["type"]

                # ---- Pitching change: update the relevant side ----
                if etype == "pitch_change":
                    diag["pitch_changes"] += 1
                    new_name = cls.get("new_pitcher") or ""
                    if is_msu_batting:
                        # Opponent made a pitching change
                        current_opp_pitcher = new_name or current_opp_pitcher
                    else:
                        # MSU made a pitching change — canonicalise against
                        # the MSU pitcher list if we can.
                        canon = _canonicalise_name(new_name, msu_pitcher_canon)
                        current_msu_pitcher = canon or new_name or current_msu_pitcher
                    continue

                # ---- Pure non-PA events (subs, timeouts, reviews) ----
                if etype == "non_pa":
                    diag["non_pa"] += 1
                    # Even non-PA events can carry a score update that
                    # we want to track (rare, but safe).
                    if ev_score:
                        away_score, home_score = ev_away, ev_home
                    continue

                # ---- Running plays (SB/CS/WP/PB/advance/out-on-bases) ----
                if etype == "running":
                    diag["running"] += 1
                    # Apply sub-event movements to bases + outs. Many
                    # running events have no sub_events and the text
                    # itself is the movement, so also parse the text
                    # directly via the sub-event parser.
                    runs, new_outs = _apply_subevents_to_bases(
                        bases, [text] + list(sub_events)
                    )
                    outs += new_outs
                    # Back-credit running-play outs to the most recent MSU
                    # pitcher PA in this half so IP per pitcher matches
                    # NCAA. Only relevant when MSU is fielding (i.e., the
                    # opponent is batting); when MSU is hitting, those
                    # outs come off MSU baserunners and don't affect MSU
                    # pitcher IP at all.
                    if (
                        not is_msu_batting
                        and new_outs > 0
                        and last_pitcher_pa_idx_this_half is not None
                    ):
                        pitcher_pas[last_pitcher_pa_idx_this_half]["outs_recorded"] = (
                            pitcher_pas[last_pitcher_pa_idx_this_half].get("outs_recorded", 0)
                            + new_outs
                        )
                    if ev_score:
                        away_score, home_score = ev_away, ev_home
                    continue

                # ---- Unknown events: log, keep state pristine ----
                if etype == "unknown":
                    diag["unknowns"] += 1
                    diag.setdefault("unknown_samples", []).append(text)
                    if ev_score:
                        away_score, home_score = ev_away, ev_home
                    continue

                # ============================================================
                # REAL PLATE APPEARANCE
                # ============================================================
                #
                # For a PA we snapshot state BEFORE applying the outcome,
                # then apply the outcome and sub_events to update bases +
                # outs + score for the next event.

                diag["pas"] += 1

                # --- Snapshot BEFORE the PA ---
                bases_before_key = _bases_to_key(bases)
                outs_before      = outs

                # Score differential from the BATTING team's perspective.
                if is_msu_batting:
                    if msu_is_home:
                        bat_team_score, field_team_score = home_score, away_score
                    else:
                        bat_team_score, field_team_score = away_score, home_score
                else:
                    if msu_is_home:
                        bat_team_score, field_team_score = away_score, home_score
                    else:
                        bat_team_score, field_team_score = home_score, away_score
                score_diff_before = bat_team_score - field_team_score

                li_value = _lookup_leverage(
                    li_table, inn_num, half_label,
                    bases_before_key, min(outs_before, 2), score_diff_before,
                )
                if li_value is not None:
                    diag["leverage_hits"] += 1
                else:
                    diag["leverage_misses"] += 1
                li_bucket = _leverage_bucket(li_value)

                # Count state and two-strike flag
                count_state, two_strike = _count_state(
                    cls.get("balls"), cls.get("strikes")
                )

                # --- Build the PA record ---
                outcome = cls["outcome"]
                meta = OUTCOME_META.get(outcome, {})
                raw_batter_disp = cls.get("batter", "")  # Title-case last name

                rbi = _parse_rbi(text)

                record_base = {
                    "contestId":        contest_id,
                    "date":             date,
                    "opponent":         opponent,
                    "opponentTeamId":   opp_team_id,
                    "isSEC":            is_sec,
                    "msu_home":         msu_is_home,
                    "location":         "neutral" if is_neutral else ("home" if msu_is_home else "away"),
                    "inning":           inn_num,
                    "half":             half_label,
                    "outcome":          outcome,
                    "balls":            cls.get("balls"),
                    "strikes":          cls.get("strikes"),
                    "pitches_seen":     cls.get("pitches_seen"),
                    "count_state":      count_state,
                    "two_strike":       two_strike,
                    "outs_before":      outs_before,
                    "bases_before":     bases_before_key,
                    "score_diff_before": score_diff_before,
                    "leverage":         li_value,
                    "leverage_bucket":  li_bucket,
                    "rbi":              rbi,
                    "AB":   meta.get("AB", 0),
                    "PA":   meta.get("PA", 1),
                    "H":    meta.get("H", 0),
                    "TB":   meta.get("TB", 0),
                    "OB":   meta.get("OB", 0),
                    "BB":   meta.get("BB", 0),
                    "IBB":  meta.get("IBB", 0),
                    "HBP":  meta.get("HBP", 0),
                    "K":    meta.get("K", 0),
                    "KL":   meta.get("KL", 0),
                    "KS":   meta.get("KS", 0),
                    "SF":   meta.get("SF", 0),
                    "SAC":  meta.get("SAC", 0),
                }

                if is_msu_batting:
                    diag["msu_pas"] += 1
                    # Canonicalise MSU batter name
                    canon_batter = _canonicalise_name(raw_batter_disp, msu_batter_canon)
                    record = dict(record_base)
                    record["batter"]       = canon_batter or raw_batter_disp
                    record["pitcher"]      = current_opp_pitcher
                    record["pitcher_hand"] = _lookup_hand_side(
                        current_opp_pitcher, opp_hand_index, diag, "opp"
                    )
                    record["bat_side"]     = None  # batter handedness TBD in future
                    batter_pas.append(record)
                else:
                    diag["opp_pas"] += 1
                    # Canonicalise MSU pitcher (the one we're tracking on our side)
                    canon_pitcher = _canonicalise_name(current_msu_pitcher, msu_pitcher_canon)
                    record = dict(record_base)
                    # NCAA asymmetry: the pitcher's "BF" column (from which
                    # we derive displayed PA = BF - SHA) INCLUDES catcher's
                    # interference PAs, and the derived "AB-against" column
                    # (BF - BB - HB - SHA - SFA) ALSO includes them. The
                    # batter side, by contrast, EXCLUDES CI from both PA
                    # and AB. So on the pitcher side we promote CI to a
                    # full PA and AB so the per-pitcher totals reconcile.
                    if outcome == "ci":
                        record["PA"] = 1
                        record["AB"] = 1
                    record["batter"]       = raw_batter_disp  # opponent hitter, no canon lookup
                    record["pitcher"]      = canon_pitcher or current_msu_pitcher
                    record["pitcher_hand"] = _lookup_hand_side(
                        current_msu_pitcher, msu_hand_index, diag, "msu"
                    )
                    # Resolve the opponent batter's bat side from the
                    # opposing team's roster, if available. Used by the
                    # pitcher splits page's vs LHB/RHB filter. Falls back
                    # to None if the lookup misses (the splits page treats
                    # null as "unknown" and drops it from L/R-only views).
                    record["bat_side"]     = _lookup_hand_side(
                        raw_batter_disp, opp_bat_index or {}, diag, "opp_bat"
                    )
                    pitcher_pas.append(record)
                    # Track the index so the next running-play event in
                    # this same half can credit its outs to this PA's
                    # pitcher (CS/PK between PAs).
                    last_pitcher_pa_idx_this_half = len(pitcher_pas) - 1

                # --- Apply the PA to the running game state ---
                #
                # 1. Place batter on the appropriate base from the outcome
                # 2. Walk sub_events to move other runners
                # 3. Increment outs from the PA (K, GO, FO, LO, PO, SF, SAC, FC, GIDP)
                # 4. Account for GIDP as 2 outs total (1 from batter + 1 from sub_event)
                # 5. Adopt the new score from the event's score field

                batter_key = last_name_from(raw_batter_disp)
                _apply_pa_to_bases(bases, batter_key, outcome)

                _, sub_outs = _apply_subevents_to_bases(bases, sub_events)
                outs += sub_outs

                # Batter's own out count (doesn't include runner outs)
                batter_out_outcomes = {
                    "k_looking", "k_swinging", "k_other",
                    "grounded_out", "out_at_first", "flied_out", "lined_out",
                    "popped", "infield_fly", "fouled_out",
                    "sac_fly", "sac_bunt",
                    "gidp", "fc",
                }
                if outcome in batter_out_outcomes:
                    outs += 1

                # Backfill outs_recorded onto the just-appended PA record so
                # the splits page can compute innings pitched. This is the
                # full PA delta = sub-event runner outs + the batter's own
                # out (if any). For GIDPs the sub-event scan picks up the
                # second out, so this naturally lands on 2.
                outs_recorded = outs - outs_before
                if is_msu_batting:
                    if batter_pas:
                        batter_pas[-1]["outs_recorded"] = outs_recorded
                else:
                    if pitcher_pas:
                        pitcher_pas[-1]["outs_recorded"] = outs_recorded

                # Home run: batter also scores. We don't need to track the
                # batter on bases (they're not on any base), but the score
                # field already reflects the run so the next score update
                # below is what matters.

                if ev_score:
                    away_score, home_score = ev_away, ev_home

    return batter_pas, pitcher_pas, diag


def _canonicalise_name(raw_name, canon_map):
    """
    Snap a PBP name to a canonical display form using a {last_name: title}
    lookup, with Levenshtein-1 fuzzy fallback for typo tolerance.

    Used for both MSU batters ("Nunnalee" -> "Nunnallee") and MSU pitchers
    ("Cijntje" canonicalisation etc.). Opposing players are NOT
    canonicalised here — we only have canon maps for our own roster.

    Args:
        raw_name: Whatever the PBP text gave us (full name or last-only).
        canon_map: {lowercase_last_name: "Title"} dict, or None.

    Returns:
        The canonical name string, or raw_name unchanged if no match.
    """
    if not raw_name or not canon_map:
        return raw_name
    ln = last_name_from(raw_name)
    if not ln:
        return raw_name
    if ln in canon_map:
        return canon_map[ln]
    for cand_ln, cand_name in canon_map.items():
        if _levenshtein1(ln, cand_ln):
            return cand_name
    return raw_name


def _lookup_hand_side(pitcher_name, hand_index, diag, side):
    """
    Wrapper around _lookup_hand that routes hit/miss counters to the
    correct diagnostic bucket (opp vs msu) so we can report the pitcher-
    handedness resolve rate separately for each side.
    """
    # Use a side-local diag proxy so _lookup_hand's counters end up in the
    # right bucket. We do this by passing a small shim dict with the same
    # keys _lookup_hand expects, then merging after.
    proxy = {"pitcher_hand_hits": 0, "pitcher_hand_misses": 0}
    hand = _lookup_hand(pitcher_name, hand_index, proxy)
    if proxy["pitcher_hand_hits"]:
        diag[f"{side}_hand_hits"] += proxy["pitcher_hand_hits"]
    if proxy["pitcher_hand_misses"]:
        diag[f"{side}_hand_misses"] += proxy["pitcher_hand_misses"]
    return hand


def _lookup_hand(pitcher_name, hand_index, diag):
    """
    Look up a pitcher's throwing hand, trying several matching strategies.

    NCAA PBP text truncates pitcher names to about 12 characters in the
    "X to p for Y." format, which breaks exact last-name matching for
    anyone with a longer surname (e.g. "Chase Deibler" becomes "Chase
    Deible", "Ryan McLaughlin" becomes "Ryan McLaugh"). To compensate we
    fall through several strategies in order of preference:

        1. Exact last-name match       (fast path, handles 80%+)
        2. Full-name prefix match      (PBP "Chase Deible" is a prefix of
                                        roster "chase deibler")
        3. Last-name prefix match      (PBP "Deible" is a prefix of
                                        roster last name "deibler")

    Steps 2 and 3 only accept the match if there is EXACTLY ONE candidate
    on the opposing team's roster. If multiple players match the prefix
    (e.g. two "Smith"s), we bail out and return None rather than guess.

    Args:
        pitcher_name: The name string we're looking up (may be truncated).
        hand_index: Dict from build_hand_index_from_roster, with keys
            "last_name_map" and "players".
        diag: Diagnostics dict for hit/miss counters.

    Returns:
        "R", "L", "S", or None if no confident match is found.
    """
    if not pitcher_name or not hand_index:
        diag["pitcher_hand_misses"] += 1
        return None

    ln_map = hand_index.get("last_name_map") or {}
    players = hand_index.get("players") or []

    # Strategy 1: exact last-name lookup
    ln = last_name_from(pitcher_name)
    hand = ln_map.get(ln)
    if hand:
        diag["pitcher_hand_hits"] += 1
        return hand

    # Strategy 2: full-name prefix match
    # NCAA PBP text like "Chase Deible to p for ..." is a 12-character
    # truncation of "Chase Deibler". Normalise the incoming name to
    # lowercase and look for roster entries whose full name STARTS WITH
    # our string. Accept only a single candidate to avoid guessing.
    needle_full = pitcher_name.lower().strip().rstrip(".")
    if len(needle_full) >= 4:
        matches = [
            (full_ln, hand)
            for (full_ln, _, hand) in players
            if full_ln.startswith(needle_full) and hand
        ]
        if len(matches) == 1:
            diag["pitcher_hand_hits"] += 1
            return matches[0][1]

    # Strategy 3: last-name prefix match
    # For "Deible" alone (or "Lacourcie" truncated), try matching against
    # roster LAST names. Again, only accept a unique match.
    if ln and len(ln) >= 4:
        matches = [
            (cand_ln, hand)
            for (_, cand_ln, hand) in players
            if cand_ln.startswith(ln) and hand
        ]
        if len(matches) == 1:
            diag["pitcher_hand_hits"] += 1
            return matches[0][1]

    # Strategy 4: punctuation-stripped matching
    # NCAA PBP drops apostrophes/hyphens from names — "O'Shaughnessy"
    # becomes "Oshaughnessy", "Smith-Jones" becomes "SmithJones".
    # Strip all non-alphanumeric characters from both sides and retry
    # exact and prefix matching on last names.
    import re as _re
    strip = lambda s: _re.sub(r"[^a-z]", "", s.lower()) if s else ""
    ln_stripped = strip(ln)
    if ln_stripped and len(ln_stripped) >= 4:
        # Exact stripped match
        for (_, cand_ln, hand) in players:
            if strip(cand_ln) == ln_stripped and hand:
                diag["pitcher_hand_hits"] += 1
                return hand
        # Prefix stripped match (unique only)
        matches = [
            (cand_ln, hand)
            for (_, cand_ln, hand) in players
            if strip(cand_ln).startswith(ln_stripped) and hand
        ]
        if len(matches) == 1:
            diag["pitcher_hand_hits"] += 1
            return matches[0][1]

    # Strategy 5: Levenshtein-1 fuzzy match on last names
    # Catches single-character typos/variants like "Hollway" vs "Holloway".
    # Only accepts a unique match to avoid guessing.
    if ln and len(ln) >= 4:
        matches = [
            (cand_ln, hand)
            for (_, cand_ln, hand) in players
            if _levenshtein1(ln, cand_ln) and ln != cand_ln and hand
        ]
        if len(matches) == 1:
            diag["pitcher_hand_hits"] += 1
            return matches[0][1]

    # Strategy 6: glued-initial disambiguation
    # NCAA PBP sometimes mangles "Johnson, J." into "Johnsonj" — the first
    # initial gets glued onto the last name as a trailing lowercase letter.
    # Detect this pattern and match against roster full names by initial.
    if ln and len(ln) >= 3 and ln[-1].isalpha() and ln[-1] == ln[-1]:
        base_ln = ln[:-1]           # "johnsonj" -> "johnson"
        initial = ln[-1]            # "j"
        if base_ln in ln_map or any(cand == base_ln for (_, cand, _) in players):
            # Find roster entries whose last name matches the base AND
            # whose first name starts with the initial.
            matches = [
                (full, hand)
                for (full, cand_ln, hand) in players
                if cand_ln == base_ln and hand
                and full.split()[0].lower().startswith(initial)
            ]
            if len(matches) == 1:
                diag["pitcher_hand_hits"] += 1
                return matches[0][1]

    diag["pitcher_hand_misses"] += 1
    return None


# ===========================================================================
# Validation
# ===========================================================================

def validate_against_ncaa(records, hitting_stats):
    """
    Sum up PA-derived counters per batter and compare against the scraped
    NCAA hitting totals. Prints a small diff table. This is the same idea
    as Zach's "Stat Check" tab.

    Args:
        records: Flat list of PA records from walk_game (all games combined).
        hitting_stats: Loaded public/data/hitting-stats-2026.json dict, or None.

    Returns:
        Dict with per-batter diffs for test-hook use.
    """
    # Per-batter totals from our parser. The new output format emits one
    # record per PA so we no longer need the is_pa gate — every record
    # in the batter_pas array is a real plate appearance.
    our_totals = defaultdict(lambda: {"AB": 0, "H": 0, "2B": 0, "3B": 0, "HR": 0,
                                       "BB": 0, "HBP": 0, "K": 0, "TB": 0, "PA": 0})
    for r in records:
        b = r.get("batter")
        if not b:
            continue
        t = our_totals[b]
        t["AB"]  += r.get("AB", 0)
        t["PA"]  += r.get("PA", 0)
        t["H"]   += r.get("H", 0)
        t["TB"]  += r.get("TB", 0)
        t["BB"]  += r.get("BB", 0)
        t["HBP"] += r.get("HBP", 0)
        t["K"]   += r.get("K", 0)
        if r["outcome"] == "double":   t["2B"] += 1
        if r["outcome"] == "triple":   t["3B"] += 1
        if r["outcome"] == "home_run": t["HR"] += 1

    if not hitting_stats:
        print("\nValidation: hitting-stats-2026.json not found — skipping diff.")
        return {}

    # Pull the "all" subset rows (player name + basic columns)
    all_subset = hitting_stats.get("all", {})
    cols = all_subset.get("columns", [])
    players = all_subset.get("players", [])
    col_idx = {c: i for i, c in enumerate(cols)}

    def col_val(row, name, default=0):
        idx = col_idx.get(name)
        if idx is None or idx >= len(row):
            return default
        raw = row[idx]
        try:
            return int(str(raw).replace(",", "").strip() or 0)
        except ValueError:
            try:
                return int(float(raw))
            except ValueError:
                return default

    print("\n=== Validation: parsed PBP totals vs NCAA hitting-stats-2026.json ===")
    print(f"{'Batter':18s} {'col':>4s} {'ours':>5s} {'ncaa':>5s} {'diff':>5s}")
    batter_diffs = {}

    # NCAA rows are "First Last" on MSU's roster. Build two indexes:
    # one exact by last-name, one by last-name prefix for fuzzy matching
    # (handles the occasional typo like "Nunnalee" in PBP vs "Nunnallee"
    # in the stats table).
    ncaa_by_last = {}
    for row in players:
        if not row:
            continue
        name = str(row[0])
        ln = last_name_from(name)
        ncaa_by_last[ln] = row

    def fuzzy_lookup(ln):
        """Find an NCAA row by last-name with a small Levenshtein tolerance."""
        if ln in ncaa_by_last:
            return ncaa_by_last[ln]
        # Cheap fuzzy: any NCAA last name within edit distance 1 of ours
        for cand, row in ncaa_by_last.items():
            if _levenshtein1(ln, cand):
                return row
        return None

    # Metric name mapping: our counter key -> NCAA column header. NCAA
    # uses "SO" for strikeouts, not "K".
    metric_to_ncaa = {
        "AB":  "AB",
        "H":   "H",
        "2B":  "2B",
        "3B":  "3B",
        "HR":  "HR",
        "BB":  "BB",
        "HBP": "HBP",
        "K":   "SO",
    }

    keys = sorted(our_totals.keys())
    for our_batter in keys:
        ln = last_name_from(our_batter)
        row = fuzzy_lookup(ln)
        if not row:
            print(f"{our_batter:18s}  NOT FOUND IN NCAA STATS")
            continue
        ours = our_totals[our_batter]
        bdiff = {}
        for metric, ncaa_col in metric_to_ncaa.items():
            n = col_val(row, ncaa_col)
            o = ours[metric]
            if o != n:
                bdiff[metric] = (o, n, o - n)
                print(f"{our_batter:18s} {metric:>4s} {o:>5d} {n:>5d} {o - n:>+5d}")
        if bdiff:
            batter_diffs[our_batter] = bdiff
        else:
            print(f"{our_batter:18s}  OK  (AB={ours['AB']}, H={ours['H']}, "
                  f"HR={ours['HR']}, BB={ours['BB']}, K={ours['K']})")

    return batter_diffs


def validate_pitchers_against_ncaa(pitcher_pas, pitching_stats):
    """
    Sum up pitcher-side counters per MSU pitcher and compare against the
    scraped NCAA pitching totals. Mirror of validate_against_ncaa but
    attributes each opponent PA to the MSU pitcher who threw it.

    We only check counters that can be derived unambiguously from PBP text
    without accounting context (ER, wild pitches that the scorer charged
    to a previous pitcher, inherited runners, etc.):
        PA, AB, H (via 1B+2B+3B+HR), BB, HBP, K, 2B, 3B, HR

    Args:
        pitcher_pas: Flat list of PA records on MSU's pitching side.
        pitching_stats: Loaded public/data/pitching-stats-2026.json dict, or None.

    Returns:
        Dict of per-pitcher diffs.
    """
    our_totals = defaultdict(lambda: {"PA": 0, "AB": 0, "H": 0, "2B": 0, "3B": 0,
                                       "HR": 0, "BB": 0, "HBP": 0, "K": 0})
    for r in pitcher_pas:
        p = r.get("pitcher")
        if not p:
            continue
        t = our_totals[p]
        t["PA"]  += r.get("PA", 0)
        t["AB"]  += r.get("AB", 0)
        t["H"]   += r.get("H", 0)
        t["BB"]  += r.get("BB", 0)
        t["HBP"] += r.get("HBP", 0)
        t["K"]   += r.get("K", 0)
        if r.get("outcome") == "double":   t["2B"] += 1
        if r.get("outcome") == "triple":   t["3B"] += 1
        if r.get("outcome") == "home_run": t["HR"] += 1

    if not pitching_stats:
        print("\nValidation: pitching-stats-2026.json not found — skipping diff.")
        return {}

    all_subset = pitching_stats.get("all", {})
    cols = all_subset.get("columns", [])
    players = all_subset.get("players", [])
    col_idx = {c: i for i, c in enumerate(cols)}

    def col_val(row, name, default=0):
        idx = col_idx.get(name)
        if idx is None or idx >= len(row):
            return default
        raw = row[idx]
        try:
            return int(str(raw).replace(",", "").strip() or 0)
        except ValueError:
            try:
                return int(float(raw))
            except ValueError:
                return default

    print("\n=== Validation: parsed PBP totals vs NCAA pitching-stats-2026.json ===")
    print(f"{'Pitcher':20s} {'col':>4s} {'ours':>5s} {'ncaa':>5s} {'diff':>5s}")
    pitcher_diffs = {}

    ncaa_by_last = {}
    for row in players:
        if not row:
            continue
        name = str(row[0])
        ln = last_name_from(name)
        ncaa_by_last[ln] = row

    def fuzzy_lookup(ln):
        if ln in ncaa_by_last:
            return ncaa_by_last[ln]
        for cand, row in ncaa_by_last.items():
            if _levenshtein1(ln, cand):
                return row
        return None

    metric_to_ncaa = {
        "PA":  "PA",
        "AB":  "AB",
        "H":   "H",
        "2B":  "2B",
        "3B":  "3B",
        "HR":  "HR",
        "BB":  "BB",
        "HBP": "HBP",
        "K":   "SO",
    }

    keys = sorted(our_totals.keys())
    any_diff = False
    for our_pitcher in keys:
        ln = last_name_from(our_pitcher)
        row = fuzzy_lookup(ln)
        if not row:
            print(f"{our_pitcher:20s}  NOT FOUND IN NCAA STATS")
            continue
        ours = our_totals[our_pitcher]
        pdiff = {}
        for metric, ncaa_col in metric_to_ncaa.items():
            n = col_val(row, ncaa_col)
            o = ours[metric]
            if o != n:
                pdiff[metric] = (o, n, o - n)
                any_diff = True
                print(f"{our_pitcher:20s} {metric:>4s} {o:>5d} {n:>5d} {o - n:>+5d}")
        if pdiff:
            pitcher_diffs[our_pitcher] = pdiff
        else:
            print(f"{our_pitcher:20s}  OK  (PA={ours['PA']}, AB={ours['AB']}, "
                  f"H={ours['H']}, BB={ours['BB']}, K={ours['K']})")

    if not any_diff and pitcher_diffs == {}:
        print("\nAll MSU pitchers reconcile against NCAA pitching-stats-2026.json.")

    return pitcher_diffs


def _levenshtein1(a, b):
    """
    Return True if strings a and b differ by at most 1 character
    (insert / delete / substitute). Cheap implementation — we only need
    to catch single-character typos like "Nunnalee" vs "Nunnallee".
    """
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    # Longest common prefix
    i = 0
    while i < min(la, lb) and a[i] == b[i]:
        i += 1
    # Longest common suffix
    j = 0
    while (j < min(la, lb) - i) and a[la - 1 - j] == b[lb - 1 - j]:
        j += 1
    # Remaining middle part after stripping prefix+suffix should have
    # length <= 1 on both sides
    return (la - i - j) <= 1 and (lb - i - j) <= 1


# ===========================================================================
# Main
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Turn cached NCAA play-by-play text into structured PA records."
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Cross-check parsed totals against scraped NCAA stats.",
    )
    args = parser.parse_args()

    # --- Load caches ---
    if not CACHE_PATH.exists():
        print(f"ERROR: {CACHE_PATH} not found. Run scrape-stats.py first.")
        sys.exit(1)

    with open(CACHE_PATH, "r", encoding="utf-8") as f:
        cache = json.load(f).get("games", {})
    print(f"Loaded {len(cache)} games from cache.")

    roster_cache = {}
    if ROSTER_CACHE_PATH.exists():
        with open(ROSTER_CACHE_PATH, "r", encoding="utf-8") as f:
            roster_cache = json.load(f).get("teams", {})
        print(f"Loaded {len(roster_cache)} team rosters from roster-cache.json.")
    else:
        print("No roster cache found — pitcher_hand will be null for all PAs.")

    # --- Load the Tango leverage table ---
    li_table = {}
    if LI_TABLE_PATH.exists():
        try:
            with open(LI_TABLE_PATH, "r", encoding="utf-8") as f:
                li_table = json.load(f)
            sections = len(li_table)
            print(f"Loaded leverage index table: {sections} half-inning sections.")
        except (json.JSONDecodeError, OSError) as e:
            print(f"WARNING: could not load leverage-index.json: {e}")
    else:
        print("No leverage-index.json — leverage/leverage_bucket will be null.")

    # Canonical MSU batter names (from the scraped hitting stats file).
    # Used to merge PBP typo variants like "Nunnalee" into the stats-table
    # spelling "Nunnallee" so aggregation groups them correctly.
    msu_batter_canon = {}
    if HITTING_STATS_PATH.exists():
        try:
            with open(HITTING_STATS_PATH, "r", encoding="utf-8") as f:
                hs = json.load(f)
            for row in hs.get("all", {}).get("players", []):
                if not row:
                    continue
                name = str(row[0])
                ln = last_name_from(name).lower()
                if ln:
                    msu_batter_canon[ln] = ln.title()
            print(
                f"Loaded {len(msu_batter_canon)} canonical MSU batter name(s) "
                f"from hitting-stats-2026.json."
            )
        except (json.JSONDecodeError, OSError) as e:
            print(f"WARNING: could not load hitting-stats-2026.json: {e}")

    # Canonical MSU pitcher names (from the scraped pitching stats file).
    # Same purpose as msu_batter_canon but for pitchers.
    msu_pitcher_canon = {}
    if PITCHING_STATS_PATH.exists():
        try:
            with open(PITCHING_STATS_PATH, "r", encoding="utf-8") as f:
                ps = json.load(f)
            for row in ps.get("all", {}).get("players", []):
                if not row:
                    continue
                name = str(row[0])
                ln = last_name_from(name).lower()
                if ln:
                    msu_pitcher_canon[ln] = ln.title()
            print(
                f"Loaded {len(msu_pitcher_canon)} canonical MSU pitcher name(s) "
                f"from pitching-stats-2026.json."
            )
        except (json.JSONDecodeError, OSError) as e:
            print(f"WARNING: could not load pitching-stats-2026.json: {e}")

    # MSU's own hand index. At the moment we don't scrape MSU's roster
    # (it's not in the opponent rosters list), so this is empty — any
    # filter that depends on opponent-batter handedness will fall back to
    # "unknown" until a follow-up task wires in MSU's roster. The splits
    # PAGE can still be built and all other filters work.
    msu_hand_index = {}

    # --- Walk games ---
    all_batter_pas  = []
    all_pitcher_pas = []
    total_diag = defaultdict(int)

    def _game_sort_key(kv):
        cid, g = kv
        return g.get("date", "")
    sorted_games = sorted(cache.items(), key=_game_sort_key)

    for cid, game_entry in sorted_games:
        tid = game_entry.get("opponentTeamId")
        team_entry = roster_cache.get(tid) if tid else None
        # Build TWO indexes per opponent team: one for throws (resolves
        # the opposing pitcher's hand for vs LHP/RHP filters on the batter
        # page) and one for bats (resolves the opposing batter's side for
        # vs LHB/RHB filters on the pitcher page).
        opp_hand_index = build_hand_index_from_roster(team_entry, field="throws")
        opp_bat_index  = build_hand_index_from_roster(team_entry, field="bats")

        bp, pp, diag = walk_game(
            game_entry,
            opp_hand_index,
            msu_hand_index,
            li_table,
            msu_batter_canon,
            msu_pitcher_canon,
            opp_bat_index=opp_bat_index,
        )
        all_batter_pas.extend(bp)
        all_pitcher_pas.extend(pp)
        for k, v in diag.items():
            if isinstance(v, list):
                total_diag.setdefault(k, []).extend(v)
            else:
                total_diag[k] += v

    # --- Summary ---
    print()
    print("=== Parsing summary ===")
    print(f"Total events walked:  {total_diag['events']}")
    print(f"  PAs total:          {total_diag['pas']}  "
          f"(MSU batting {total_diag['msu_pas']}, "
          f"MSU pitching {total_diag['opp_pas']})")
    print(f"  pitching changes:   {total_diag['pitch_changes']}")
    print(f"  running plays:      {total_diag['running']}")
    print(f"  non-PA subs/etc:    {total_diag['non_pa']}")
    print(f"  unknown:            {total_diag['unknowns']}")
    unknown_samples = total_diag.get("unknown_samples", [])
    if unknown_samples:
        print("  unknown event texts:")
        for s in unknown_samples:
            print(f"    - {s}")

    opp_hits = total_diag["opp_hand_hits"]
    opp_miss = total_diag["opp_hand_misses"]
    if opp_hits + opp_miss:
        pct = opp_hits / (opp_hits + opp_miss) * 100
        print(f"Opponent pitcher hand resolved: "
              f"{opp_hits}/{opp_hits + opp_miss} ({pct:.1f}%)")

    msu_hits = total_diag["msu_hand_hits"]
    msu_miss = total_diag["msu_hand_misses"]
    if msu_hits + msu_miss:
        pct = msu_hits / (msu_hits + msu_miss) * 100
        print(f"MSU pitcher hand resolved:      "
              f"{msu_hits}/{msu_hits + msu_miss} ({pct:.1f}%)  "
              f"[requires MSU roster scrape — follow-up]")

    bat_hits = total_diag["opp_bat_hand_hits"]
    bat_miss = total_diag["opp_bat_hand_misses"]
    if bat_hits + bat_miss:
        pct = bat_hits / (bat_hits + bat_miss) * 100
        print(f"Opp batter bat side resolved:   "
              f"{bat_hits}/{bat_hits + bat_miss} ({pct:.1f}%)")

    lev_hits = total_diag["leverage_hits"]
    lev_miss = total_diag["leverage_misses"]
    if lev_hits + lev_miss:
        pct = lev_hits / (lev_hits + lev_miss) * 100
        print(f"Leverage index resolved:        "
              f"{lev_hits}/{lev_hits + lev_miss} ({pct:.1f}%)")

    # --- Load hitting/pitching stats for name resolution ---
    hitting_stats = None
    pitching_stats = None
    if HITTING_STATS_PATH.exists():
        with open(HITTING_STATS_PATH, "r", encoding="utf-8") as f:
            hitting_stats = json.load(f)
    if PITCHING_STATS_PATH.exists():
        with open(PITCHING_STATS_PATH, "r", encoding="utf-8") as f:
            pitching_stats = json.load(f)

    # --- Resolve batter names from last-name to full name ---
    if hitting_stats:
        batter_name_map = {}
        for row in hitting_stats.get("all", {}).get("players", []):
            if row:
                full_name = row[0]  # e.g. "Ace Reese"
                last_name = full_name.split()[-1].lower()  # e.g. "reese"
                batter_name_map[last_name] = full_name

        for pa in all_batter_pas:
            if pa.get("batter"):
                last_name_key = pa["batter"].lower()
                if last_name_key in batter_name_map:
                    pa["batter"] = batter_name_map[last_name_key]

    # --- Resolve pitcher names from last-name to full name ---
    if pitching_stats:
        pitcher_name_map = {}
        for row in pitching_stats.get("all", {}).get("players", []):
            if row:
                full_name = row[0]  # e.g. "Tomas Valincius" or "Chris Billingsley Jr."
                # Extract last name, skipping generational suffixes (Jr., Sr., II, III, etc.)
                parts = full_name.split()
                last_name = parts[-1].lower()
                # If the last part is a suffix, use the second-to-last part instead
                if last_name in ("jr.", "sr.", "ii", "iii", "iv"):
                    if len(parts) >= 2:
                        last_name = parts[-2].lower()
                pitcher_name_map[last_name] = full_name

        for pa in all_pitcher_pas:
            if pa.get("pitcher"):
                last_name_key = pa["pitcher"].lower()
                if last_name_key in pitcher_name_map:
                    pa["pitcher"] = pitcher_name_map[last_name_key]

    # --- Write output ---
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "version":       2,
            "gamesIncluded": len(cache),
            "batter_pas":    all_batter_pas,
            "pitcher_pas":   all_pitcher_pas,
        }, f)
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"\nWrote {OUTPUT_PATH} "
          f"({len(all_batter_pas)} batter PAs, "
          f"{len(all_pitcher_pas)} pitcher PAs, {size_kb:.1f} KB).")

    # --- Validate ---
    if args.validate:
        validate_against_ncaa(all_batter_pas, hitting_stats)
        validate_pitchers_against_ncaa(all_pitcher_pas, pitching_stats)


if __name__ == "__main__":
    main()
