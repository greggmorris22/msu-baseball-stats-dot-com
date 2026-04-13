"""
settings.py — League configuration and API constants.

All league-specific rules, URLs, and app-wide constants live here.
Nothing else in the codebase hard-codes league rules or API URLs.
"""

import os

# ---------------------------------------------------------------------------
# Fantrax league identity
# ---------------------------------------------------------------------------
LEAGUE_ID = os.getenv("FANTRAX_LEAGUE_ID", "eofqrg7umiyswern")
USER_SECRET_ID = os.getenv("FANTRAX_USER_SECRET_ID", "3ou0yavll903us7u")

# "My Team" — set this to your team name (as it appears in Fantrax).
# Used to highlight your team in rankings and pre-fill the Trade Calculator.
# Can be overridden at runtime via the sidebar dropdown.
MY_TEAM_NAME = os.getenv("FANTRAX_MY_TEAM", "Uncle Ben's Rice")

# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------
FANTRAX_BASE_URL = "https://www.fantrax.com/fxea/general/"

# FanGraphs projection pages (data is embedded in __NEXT_DATA__ script tag)
# The old /api/projections JSON endpoint no longer returns data — FanGraphs
# migrated to Next.js server-side rendering in late 2025.
FG_BAT_URL = (
    "https://www.fangraphs.com/projections"
    "?type=steamer&stats=bat&pos=all&team=0&players=0&lg=all"
)
FG_PIT_URL = (
    "https://www.fangraphs.com/projections"
    "?type=steamer&stats=pit&pos=all&team=0&players=0&lg=all"
)

# OOPSY projection URLs — same page structure, different projection system.
# OOPSY ("Out Of Position Scoring Year") is a newer FanGraphs projection system
# that incorporates bat speed for hitters and Stuff+ for pitchers.
FG_OOPSY_BAT_URL = (
    "https://www.fangraphs.com/projections"
    "?type=oopsy&stats=bat&pos=all&team=0&players=0&lg=all"
)
FG_OOPSY_PIT_URL = (
    "https://www.fangraphs.com/projections"
    "?type=oopsy&stats=pit&pos=all&team=0&players=0&lg=all"
)

# Map of projection system key -> (batter_url, pitcher_url)
# Used by the UI to switch between projection sources.
PROJECTION_SYSTEMS = {
    "steamer": {
        "label": "2026 Steamer (Preseason)",
        "bat_url": FG_BAT_URL,
        "pit_url": FG_PIT_URL,
    },
    "oopsy": {
        "label": "OOPSY Projections",
        "bat_url": FG_OOPSY_BAT_URL,
        "pit_url": FG_OOPSY_PIT_URL,
    },
}

# ---------------------------------------------------------------------------
# Stat categories
# ---------------------------------------------------------------------------
BATTING_CATS = ["R", "HR", "RBI", "TB", "OBP", "NSB"]
PITCHING_CATS = ["IP", "WHIP", "K", "ERA", "QA3", "NSV"]

# These categories penalize high values — Z-scores are negated
LOWER_IS_BETTER = ["ERA", "WHIP"]

# All 12 category Z-score column names as stored in DuckDB
ALL_CAT_Z_COLS = [
    "r_z", "hr_z", "rbi_z", "tb_z", "obp_z", "nsb_z",
    "ip_z", "whip_z", "k_z", "era_z", "qa3_z", "nsv_z",
]

# ---------------------------------------------------------------------------
# Roster configuration
# ---------------------------------------------------------------------------
# Active roster slot counts (excludes IR and Minors which are unlimited)
ACTIVE_SLOTS = {
    "C": 1, "1B": 1, "2B": 1, "3B": 1, "SS": 1,
    "OF": 3, "Util": 1, "SP": 5, "RP": 2, "BN": 8,
}

WEEKLY_START_LIMIT = 10  # Max SP starts per week

# Position tabs shown on the Player Rankings page
POSITION_TABS = ["All", "C", "1B", "2B", "3B", "SS", "OF", "SP", "RP"]

# Greedy lineup optimizer fills these slots in this priority order.
# Scarce/specialist positions are filled before flexible ones.
LINEUP_PRIORITY = ["C", "SS", "2B", "3B", "1B", "OF", "OF", "OF", "SP", "SP", "SP", "SP", "SP", "RP", "RP"]

# ---------------------------------------------------------------------------
# Z-score player pool sizing
# ---------------------------------------------------------------------------
# Number of rosterable players estimated per team.
# These factors determine the "relevant player pool" for mean/std calculations.
# Hitter pool:  12 teams × 14 hitters  = ~168
# SP pool:      12 teams × 7 SPs       = ~84  (5 roster slots + 2 bench depth)
# RP pool:      12 teams × 3 RPs       = ~36  (2 roster slots + 1 bench depth)
#
# SP and RP are computed as separate pools because lumping them together
# dramatically overvalues RPs: closers get huge NSV Z-scores when compared
# to an SP-dominated pool where most pitchers have 0 saves.
HITTER_POOL_FACTOR = 14
SP_POOL_FACTOR = 7
RP_POOL_FACTOR = 3

# Legacy combined pitcher pool factor — kept for reference but no longer used
# in the Z-score engine. SP_POOL_FACTOR and RP_POOL_FACTOR replaced it.
PITCHER_POOL_FACTOR = 10

# ---------------------------------------------------------------------------
# RP Z-score scaling
# ---------------------------------------------------------------------------
# Relief pitchers are computed against their own RP-only pool, which gives them
# Z-scores on the same scale as SPs computed against the SP pool.  But because
# you only roster 2 RPs (vs 5 SPs), an RP's contribution to your fantasy team
# is inherently less valuable than an SP's.
#
# This factor scales ALL RP pitching Z-scores down so that the best closer
# lands at roughly the 30th overall rank instead of competing with ace SPs.
# Tuning guide: raise the number to push RPs up the rankings, lower to push
# them down.  A value of 1.0 means no scaling (RP Z = SP Z on same scale).
RP_Z_SCALE_FACTOR = 0.17

# Fallback if API doesn't return a valid team count
DEFAULT_NUM_TEAMS = 12
NUM_TEAMS_VALID_RANGE = (4, 20)

# ---------------------------------------------------------------------------
# Z-score computation tuning
# ---------------------------------------------------------------------------
# Minimum standard deviation for Z-score categories. Prevents division-by-zero
# or extreme Z-scores when most players in the pool share the same value (e.g.,
# most pitchers have 0 saves, giving NSV a near-zero std dev).
# Any category whose pool std dev is below this floor uses the floor instead.
MIN_Z_STD = 0.5

# ---------------------------------------------------------------------------
# Caching TTLs (in hours)
# ---------------------------------------------------------------------------
CACHE_TTL_HOURS = 6         # Projections — updated daily at most
ROSTER_CACHE_TTL_HOURS = 1  # Rosters — waiver activity happens throughout the day
