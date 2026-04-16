"""
scrape-stats.py — Scrape NCAA individual stats AND situational splits for
Mississippi State baseball.

This is the combined scraper that visits four pages per game:
  1. /contests/{id}/individual_stats — hitting, pitching, fielding box score
  2. /contests/{id}/box_score — pitcher decisions (W/L/SV) and opponent runs
  3. /contests/{id}/situational_stats — situational splits (same as scrape-splits.py)
  4. /contests/{id}/play_by_play — inning-by-inning event log (cached for later use)

Outputs five JSON files into public/data/:
  - hitting-stats-2026.json    (individual hitting stats + derived metrics)
  - pitching-stats-2026.json   (individual pitching stats + derived metrics)
  - fielding-stats-2026.json   (individual fielding stats)
  - hitting-splits-2026.json   (situational splits — same format as before)
  - pitching-splits-2026.json  (situational splits — same format as before)

Usage:
    # Headed (local — bypasses NCAA bot detection via visible Chrome window)
    .venv/Scripts/python.exe scripts/scrape-stats.py

    # Headless (CI/GitHub Actions — attempts stealth evasion)
    .venv/Scripts/python.exe scripts/scrape-stats.py --headless

Requirements:
    pip install playwright beautifulsoup4
    playwright install chromium
"""

import argparse
import json
import re
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# SEC standings scraper — runs as the final step of the main scrape.
# The filename uses a hyphen (scrape-sec-standings.py) so we use importlib
# to load it, since Python module names cannot contain hyphens.
import importlib.util as _ilu
_spec = _ilu.spec_from_file_location(
    "scrape_sec_standings",
    Path(__file__).parent / "scrape-sec-standings.py",
)
_mod = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
scrape_sec_standings = _mod.scrape_sec_standings


# ===========================================================================
# Configuration
# ===========================================================================

# MSU's 2025-26 team page on stats.ncaa.org
TEAM_URL = "https://stats.ncaa.org/teams/614666"

# The name used in table headings to identify MSU's tables (vs. opponent's)
TEAM_NAME = "Mississippi St."

# Output directory (relative to this script's location)
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR.parent / "public" / "data"

# Cache directory and file — stores raw per-game data to avoid re-scraping
# This is a development artifact (not served publicly). Add to .gitignore if desired.
CACHE_DIR  = SCRIPT_DIR.parent / "data"
CACHE_PATH = CACHE_DIR / "scrape-cache.json"

# Roster cache — one entry per unique opponent team ID we've seen. Stores
# pitcher names and their throwing hand. Refreshed on --full re-scrapes.
ROSTER_CACHE_PATH = CACHE_DIR / "roster-cache.json"

# Public output file mapping pitcher name -> handedness, grouped by team ID.
# Consumed by parse_pbp.py (and indirectly by the Splits Selector page).
PITCHER_HAND_OUT_PATH = OUTPUT_DIR / "pitcher-handedness-2026.json"

# Delay between page loads (seconds) — reduced from 1.5 since we load 3 pages/game
REQUEST_DELAY = 1.0

# SEC teams (2024-25 membership, includes Texas and Oklahoma)
SEC_TEAMS = {
    "Alabama", "Arkansas", "Auburn", "Florida", "Georgia",
    "Kentucky", "LSU", "Mississippi St.", "Missouri", "Ole Miss",
    "Oklahoma", "South Carolina", "Tennessee", "Texas",
    "Texas A&M", "Vanderbilt",
}


# ===========================================================================
# IP (Innings Pitched) Conversion Utilities
# ===========================================================================

def ip_to_thirds(ip_str):
    """
    Convert an innings pitched string like '4.1' to total thirds of an inning.

    Baseball uses a quirky notation where the decimal part is thirds,
    not tenths. So 4.1 means "4 and 1/3 innings" = 13 thirds total.

    Args:
        ip_str: String like "4.1", "4.2", "4.0", "4", or "".

    Returns:
        Integer count of thirds. E.g. "4.1" -> 13, "4.2" -> 14, "4.0" -> 12.
    """
    if not ip_str or ip_str.strip() == "":
        return 0
    parts = ip_str.strip().split(".")
    whole = int(parts[0]) if parts[0] else 0
    partial = int(parts[1]) if len(parts) > 1 and parts[1] else 0
    return whole * 3 + partial


def thirds_to_ip(total_thirds):
    """
    Convert total thirds back to baseball IP notation string.

    Args:
        total_thirds: Integer count of thirds. E.g. 13.

    Returns:
        String like "4.1". E.g. 13 -> "4.1", 14 -> "4.2", 12 -> "4.0".
    """
    whole = total_thirds // 3
    partial = total_thirds % 3
    return f"{whole}.{partial}"


def thirds_to_float(total_thirds):
    """
    Convert total thirds to a float for arithmetic.

    Args:
        total_thirds: Integer count of thirds. E.g. 13.

    Returns:
        Float. E.g. 13 -> 4.333...
    """
    return total_thirds / 3.0


# ===========================================================================
# Helpers — Reused from scrape-splits.py
# ===========================================================================

def parse_split_cell(text):
    """
    Parse a situational stats cell like "3-8" into (hits, at_bats).
    Returns (0, 0) for empty cells or unparseable values.

    Args:
        text: Raw cell text, e.g. "3-8", "", or whitespace.

    Returns:
        Tuple of (hits: int, at_bats: int).
    """
    text = text.strip()
    if not text:
        return (0, 0)
    match = re.match(r"^(\d+)-(\d+)$", text)
    if match:
        return (int(match.group(1)), int(match.group(2)))
    return (0, 0)


def is_sec_opponent(opponent_text):
    """
    Check if an opponent name matches an SEC team.

    The schedule page shows opponent names that may include prefixes like
    '@' (away game) or '#5' (ranking), so we check if any SEC team name
    appears as a substring of the opponent text.

    Args:
        opponent_text: Raw opponent text from the schedule, e.g.
                       "@ #5 Georgia" or "Alabama".

    Returns:
        True if the opponent is an SEC team.
    """
    text = opponent_text.strip()
    for team in SEC_TEAMS:
        if team == TEAM_NAME:
            continue
        if team in text:
            return True
    return False


def extract_game_info(page):
    """
    Extract game information from the team schedule page.

    Parses the schedule table to find completed games (those with a box
    score link) and returns metadata for each: contest ID, date, opponent
    name, opponent team ID (for roster lookups), and whether the opponent
    is an SEC team.

    Args:
        page: Playwright page object on the team schedule URL.

    Returns:
        List of dicts, each with keys:
          - contestId (str): e.g. "6493958"
          - date (str): e.g. "04/02/2026"
          - opponent (str): e.g. "Georgia"
          - opponentTeamId (str or None): NCAA team ID like "614619", used for
                roster scraping. None if no link in the schedule row.
          - isSEC (bool): whether the opponent is an SEC team
    """
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    games = []

    for link in soup.find_all("a", href=re.compile(r"/contests/\d+/box_score")):
        match = re.search(r"/contests/(\d+)/box_score", link["href"])
        if not match:
            continue

        contest_id = match.group(1)
        row = link.find_parent("tr")
        if not row:
            continue

        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        date_text = cells[0].get_text(strip=True)
        opponent_cell = cells[1]
        opponent_text = opponent_cell.get_text(strip=True)
        opponent_clean = re.sub(r"^@\s*", "", opponent_text)
        opponent_clean = re.sub(r"^#\d+\s*", "", opponent_clean)

        # The opponent cell usually contains a link like
        #   <a href="/teams/614619">Hofstra</a>
        # Some rows may have no link (e.g. non-D1 opponents) — in that case
        # we can't scrape their roster, so we fall back to None.
        opponent_team_id = None
        opp_link = opponent_cell.find("a", href=re.compile(r"/teams/\d+"))
        if opp_link:
            id_match = re.search(r"/teams/(\d+)", opp_link["href"])
            if id_match:
                opponent_team_id = id_match.group(1)

        games.append({
            "contestId": contest_id,
            "date": date_text,
            "opponent": opponent_clean,
            "opponentTeamId": opponent_team_id,
            "isSEC": is_sec_opponent(opponent_text),
        })

    return games


def extract_full_schedule(page):
    """
    Extract the full schedule (played AND unplayed games) from the team page.

    Parses every row in the schedule table, not just completed games.
    For each game, captures: date, opponent, whether it's an away game,
    and the contest ID (if the game has been played and has a box score).

    The result/score and attendance are derived later from cached box-score
    data, not scraped here (the NCAA schedule table doesn't show scores inline).

    Args:
        page: Playwright page object on the team schedule URL.

    Returns:
        List of dicts with keys:
          - date (str): e.g. "02/14/2026" or "02/14/2026(1)" for doubleheaders
          - opponent (str): cleaned opponent name (ranking prefixes removed)
          - isAway (bool): True if the opponent text started with "@"
          - isSEC (bool): whether the opponent is an SEC team
          - contestId (str or None): present only for completed games
    """
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    schedule = []

    # Find the schedule table — it's the main content table with game rows.
    # Look for all <tr> that contain <td> cells (skip header rows).
    # The schedule table lives inside the team page's main content area.
    for tr in soup.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 2:
            continue

        date_text = cells[0].get_text(strip=True)
        # Skip rows that don't look like dates (e.g. header rows, empty rows)
        if not re.match(r"\d{2}/\d{2}/\d{4}", date_text):
            continue

        opponent_cell = cells[1]
        opponent_text = opponent_cell.get_text(strip=True)

        # Detect away games (opponent prefixed with "@")
        is_away = opponent_text.strip().startswith("@")

        # Clean opponent name: remove "@", ranking prefix
        opponent_clean = re.sub(r"^@\s*", "", opponent_text)
        opponent_clean = re.sub(r"^#\d+\s*", "", opponent_clean)

        # Check for box_score link (completed game)
        contest_id = None
        box_link = tr.find("a", href=re.compile(r"/contests/\d+/box_score"))
        if box_link:
            id_match = re.search(r"/contests/(\d+)/box_score", box_link["href"])
            if id_match:
                contest_id = id_match.group(1)

        schedule.append({
            "date": date_text,
            "opponent": opponent_clean,
            "isAway": is_away,
            "isSEC": is_sec_opponent(opponent_text),
            "contestId": contest_id,
        })

    return schedule


def find_team_tables(html, team_name, table_type):
    """
    Find the correct table for a given team and type on an NCAA stats page.

    The NCAA page structures each section as a Bootstrap "card":
        <div class="card">
          <div class="card-header">Mississippi St.Hitting</div>
          <div class="card-body">
            <table>...</table>
          </div>
        </div>

    Works for individual_stats pages (Hitting, Pitching, Fielding) and
    situational_stats pages (Hitting, Pitching).

    Args:
        html: Full page HTML string.
        team_name: e.g. "Mississippi St."
        table_type: "Hitting", "Pitching", or "Fielding"

    Returns:
        BeautifulSoup <table> element, or None if not found.
    """
    soup = BeautifulSoup(html, "html.parser")

    for header in soup.find_all("div", class_="card-header"):
        header_text = header.get_text(strip=True)
        if team_name in header_text and table_type in header_text:
            card = header.parent
            if card:
                table = card.find("table")
                if table:
                    return table

    return None


# ===========================================================================
# Situational Stats Parsing (reused from scrape-splits.py)
# ===========================================================================

def parse_situational_table(table_element):
    """
    Parse one situational stats HTML table into a list of player dicts.

    Each table has a header row with column names (Player, Pos, split1, ...),
    data rows with player name, position, and "H-AB" cells, and a totals row.

    Args:
        table_element: A BeautifulSoup <table> element.

    Returns:
        Tuple of (columns: list[str], players: list[dict]).
        Each player dict has keys: 'name', 'pos', 'splits' (dict of col -> (h, ab)).
    """
    rows = table_element.find_all("tr")
    if len(rows) < 2:
        return [], []

    header_cells = rows[0].find_all(["th", "td"])
    columns = [cell.get_text(strip=True) for cell in header_cells]

    players = []
    for row in rows[1:]:
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue

        name = cells[0].get_text(strip=True)
        if not name:
            continue

        pos = cells[1].get_text(strip=True) if len(cells) > 1 else ""

        splits = {}
        for i in range(2, min(len(cells), len(columns))):
            col_name = columns[i]
            h, ab = parse_split_cell(cells[i].get_text(strip=True))
            splits[col_name] = (h, ab)

        players.append({
            "name": name,
            "pos": pos,
            "splits": splits,
        })

    split_columns = columns[2:] if len(columns) > 2 else []
    return split_columns, players


def aggregate_splits(all_game_data, game_indices=None):
    """
    Aggregate split stats across all games (or a subset) for each player.

    Args:
        all_game_data: List of (columns, players) tuples from each game.
        game_indices: Optional set of indices into all_game_data to include.
                      If None, all games are included.

    Returns:
        Dict with keys:
          - 'columns': list of split column names (union of all games)
          - 'players': list of player dicts with aggregated splits
          - 'totals': aggregated team totals dict
    """
    seen_columns = {}
    for idx, (columns, _) in enumerate(all_game_data):
        if game_indices is not None and idx not in game_indices:
            continue
        for col in columns:
            if col not in seen_columns:
                seen_columns[col] = len(seen_columns)
    all_columns = sorted(seen_columns.keys(), key=lambda c: seen_columns[c])

    player_totals = {}
    team_totals = {}

    for idx, (columns, players) in enumerate(all_game_data):
        if game_indices is not None and idx not in game_indices:
            continue
        for p in players:
            name = p["name"]
            is_team_row = TEAM_NAME.lower() in name.lower()

            if is_team_row:
                for col, (h, ab) in p["splits"].items():
                    if col not in team_totals:
                        team_totals[col] = [0, 0]
                    team_totals[col][0] += h
                    team_totals[col][1] += ab
            else:
                if name not in player_totals:
                    player_totals[name] = {"pos": p["pos"], "splits": {}}
                if p["pos"] and not player_totals[name]["pos"]:
                    player_totals[name]["pos"] = p["pos"]
                for col, (h, ab) in p["splits"].items():
                    if col not in player_totals[name]["splits"]:
                        player_totals[name]["splits"][col] = [0, 0]
                    player_totals[name]["splits"][col][0] += h
                    player_totals[name]["splits"][col][1] += ab

    players_list = []
    for name, data in player_totals.items():
        total_ab = sum(v[1] for v in data["splits"].values())
        if total_ab > 0:
            players_list.append({
                "name": name,
                "pos": data["pos"],
                "splits": data["splits"],
            })

    def total_ab(p):
        return sum(v[1] for v in p["splits"].values())
    players_list.sort(key=total_ab, reverse=True)

    return {
        "columns": all_columns,
        "players": players_list,
        "totals": team_totals,
    }


# ===========================================================================
# Individual Stats Parsing (NEW)
# ===========================================================================

def parse_individual_table(table_element):
    """
    Parse an NCAA individual stats table (hitting, pitching, or fielding).

    Reads column headers from <thead> and player rows from <tbody>.
    The last row of <tbody> is the team totals row.

    Args:
        table_element: A BeautifulSoup <table> element from the individual_stats page.

    Returns:
        Tuple of (headers_list, player_rows_list) where:
          - headers_list: list of column header strings (excluding "#")
          - player_rows_list: list of dicts, each mapping header name to cell value (string)
            Each dict also has an '_is_totals' key indicating if it's the summary row,
            and '_row_index' indicating the original row position (0-based).
    """
    if not table_element:
        return [], []

    # --- Extract headers from <thead> ---
    thead = table_element.find("thead")
    if not thead:
        return [], []

    header_row = thead.find("tr")
    if not header_row:
        return [], []

    raw_headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]

    # Build a mapping from raw column index to header name, skipping "#"
    # We keep "Name" and all stat columns.
    header_map = {}  # raw_index -> header_name
    clean_headers = []
    for i, h in enumerate(raw_headers):
        if h == "#":
            continue  # Skip jersey number column
        header_map[i] = h
        clean_headers.append(h)

    # --- Extract player rows from <tbody> ---
    tbody = table_element.find("tbody")
    if not tbody:
        return clean_headers, []

    rows = tbody.find_all("tr")
    player_rows = []

    for row_idx, row in enumerate(rows):
        cells = row.find_all(["th", "td"])
        player_data = {}

        for raw_i, cell in enumerate(cells):
            if raw_i in header_map:
                col_name = header_map[raw_i]
                player_data[col_name] = cell.get_text(strip=True)

        if not player_data.get("Name"):
            continue

        # Detect if this is the team totals row (last row, or name matches team)
        is_totals = (
            TEAM_NAME.lower() in player_data["Name"].lower()
            or row_idx == len(rows) - 1
        )
        player_data["_is_totals"] = is_totals
        player_data["_row_index"] = row_idx

        player_rows.append(player_data)

    return clean_headers, player_rows


def parse_box_score_decisions(html, team_name):
    """
    Parse pitcher decisions (W/L/SV) from the box score page.

    The NCAA box score page structures each decision like this (with many
    intervening newlines and sometimes a stray unicode character):

        Winning Pitcher
            [blank lines and junk]
        Paul Farley (3-0)2.0 IP, 2 H, 0 R, 1 K, 2 BB

        Save
            [blank lines]
        Matt Scott (1)1.2 IP, 1 H, 0 R, 1 K, 0 BB

        Losing Pitcher
            [blank lines]
        Tomas Valincius (4-1)7.1 IP, 4 H, 2 R, 10 K, 0 BB

    Key things to notice:
    - The stats (e.g. "2.0 IP...") come immediately after the record with NO
      whitespace — so we cannot anchor the name match on a trailing newline.
    - The Win/Loss record is formatted as "(N-N)", the Save number as "(N)".
    - The three sections can appear in any order; in particular Save often
      sits between Winning Pitcher and Losing Pitcher.

    Strategy: for each decision type, lazily skip forward from the label to
    the first "Name (record)" pattern, using a negative lookahead that
    prevents the search from crossing into another decision label.

    Args:
        html: Full page HTML string of the box_score page.
        team_name: Our team name (unused here; kept for call-site compatibility).
                   Name matching against MSU's roster happens later in
                   calculate_pitching_stats().

    Returns:
        Dict with keys "win", "loss", "save", each containing a player name
        string or None if not found.
    """
    decisions = {"win": None, "loss": None, "save": None}

    # Extract the page text for regex matching
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text()

    # Character class for names: letters plus common name punctuation
    # (dots for "J.T.", apostrophes for "O'Brien", hyphens for double-barrelled
    # names, spaces between first/last). Digits and parens are excluded so the
    # name cannot bleed into the "(N-N)" record that follows.
    NAME_CHARS = r"[A-Za-z.'\-\s]"

    # --- Winning Pitcher --------------------------------------------------
    # Label → (any junk, but don't cross into another decision label) → Name (W-L)
    win_match = re.search(
        r"Winning Pitcher"
        r"(?:(?!Losing Pitcher|Save|Scoring Summary)[\s\S])*?"
        r"([A-Z]" + NAME_CHARS + r"+?)\s*\(\d+-\d+\)",
        page_text,
    )
    if win_match:
        decisions["win"] = win_match.group(1).strip()

    # --- Losing Pitcher ---------------------------------------------------
    loss_match = re.search(
        r"Losing Pitcher"
        r"(?:(?!Winning Pitcher|Save|Scoring Summary)[\s\S])*?"
        r"([A-Z]" + NAME_CHARS + r"+?)\s*\(\d+-\d+\)",
        page_text,
    )
    if loss_match:
        decisions["loss"] = loss_match.group(1).strip()

    # --- Save -------------------------------------------------------------
    # Uses "(N)" — a single integer — not a "(N-N)" record. The negative
    # lookbehind on the "Save" label prevents accidental matches inside words
    # like "Saved" (there's no such label on the page, but it's cheap insurance).
    save_match = re.search(
        r"(?<!\w)Save"
        r"(?:(?!Winning Pitcher|Losing Pitcher|Scoring Summary)[\s\S])*?"
        r"([A-Z]" + NAME_CHARS + r"+?)\s*\(\d+\)",
        page_text,
    )
    if save_match:
        decisions["save"] = save_match.group(1).strip()

    return decisions


def parse_opponent_runs(html, team_name):
    """
    Parse the opponent's total runs from the box score linescore.

    The box score page contains a linescore table showing each team's runs
    per inning plus totals. We find the row that does NOT match our team_name
    and extract the total runs (last numeric cell or the 'R' column).

    Args:
        html: Full page HTML string of the box_score page.
        team_name: Our team name (e.g. "Mississippi St.").

    Returns:
        Integer: the opponent's total runs, or 0 if parsing fails.
    """
    soup = BeautifulSoup(html, "html.parser")

    # The linescore is typically in a table early on the page.
    # Look for a table containing both team names in separate rows.
    # The linescore table usually has headers like: Team, 1, 2, ..., R, H, E
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # Check if this looks like a linescore table by looking for "R" in headers
        header_cells = rows[0].find_all(["th", "td"])
        header_texts = [c.get_text(strip=True) for c in header_cells]

        # Find the 'R' (runs) column index
        r_index = None
        for idx, h in enumerate(header_texts):
            if h == "R":
                r_index = idx
                break

        if r_index is None:
            continue

        # Now find the opponent's row (the row that doesn't contain our team name)
        for row in rows[1:]:
            cells = row.find_all(["th", "td"])
            if len(cells) <= r_index:
                continue

            row_text = cells[0].get_text(strip=True)
            # Skip our team's row
            if team_name.lower() in row_text.lower():
                continue

            # This should be the opponent's row
            try:
                runs = int(cells[r_index].get_text(strip=True))
                return runs
            except (ValueError, IndexError):
                continue

    return 0


# ===========================================================================
# Play-by-Play Parser
# ===========================================================================

# NCAA play-by-play pages use a weird "3a" literal as a separator between
# sub-events within a single play (e.g. "Teel walked (3-2 KBBBSFB)3a Reese
# advanced to second."). ASCII 0x3A is ':' so this looks like a source-side
# encoding bug. We split on it to get cleanly-separated sub-events.
PBP_SUBEVENT_SEP = re.compile(r"3a\s+")

# Inning header text like "1st Inning", "2nd Inning", "10th Inning"
PBP_INNING_RE = re.compile(r"(\d+)(?:st|nd|rd|th)\s+Inning", re.IGNORECASE)

# Line-score summary format in the tfoot: "R: 0, H: 1, E: 0, LOB: 1"
PBP_LINESCORE_RE = re.compile(
    r"R:\s*(\d+).*?H:\s*(\d+).*?E:\s*(\d+).*?LOB:\s*(\d+)",
    re.IGNORECASE,
)


def _parse_pbp_events(cells_row_list, side):
    """
    Given a list of row cell-lists (each row is [td1, td2, td3]), extract
    the events on the given side.

    Args:
        cells_row_list: List of lists of cell text [away_text, score_text, home_text]
        side: "away" (read from td1) or "home" (read from td3)

    Returns:
        List of event dicts: {"text": ..., "sub_events": [...], "score": "X-Y"}
    """
    events = []
    col = 0 if side == "away" else 2
    for cells in cells_row_list:
        if len(cells) < 3:
            continue
        text = cells[col].strip()
        if not text:
            continue
        parts = PBP_SUBEVENT_SEP.split(text)
        main = parts[0].strip()
        subs = [p.strip() for p in parts[1:] if p.strip()]
        events.append({
            "text": main,
            "sub_events": subs,
            "score": cells[1].strip(),
        })
    return events


def _parse_linescore_cell(text):
    """
    Parse a tfoot line-score cell like "R: 0, H: 1, E: 0, LOB: 1" into a dict.
    Returns {} if the text doesn't match the expected format.
    """
    m = PBP_LINESCORE_RE.search(text or "")
    if not m:
        return {}
    return {
        "R":   int(m.group(1)),
        "H":   int(m.group(2)),
        "E":   int(m.group(3)),
        "LOB": int(m.group(4)),
    }


def parse_play_by_play(html):
    """
    Parse an NCAA play-by-play page into a structured dict of innings.

    Each inning on the page is a <div class="card"> with a <div class="card-header">
    containing "Nth Inning" and a <table class="table"> inside. The table has:
      - <thead>: one row with three <th>s — away team (with img alt), "Score",
        home team (with img alt).
      - <tbody>: rows of [away_desc, score, home_desc]. Exactly one of the
        side cells is populated per row (the other side is empty), indicating
        which half of the inning the event belongs to.
      - <tfoot class="table-dark">: summary row with "R: X, H: Y, E: Z, LOB: W"
        line-score text for each half.

    Args:
        html: Full page HTML string of the play_by_play page.

    Returns:
        Dict:
        {
          "away_team": str,
          "home_team": str,
          "innings": [
            {
              "inning": int,
              "top":    {"events": [...], "line": {"R":..., "H":..., "E":..., "LOB":...}},
              "bottom": {"events": [...], "line": {...}}
            },
            ...
          ]
        }

        Returns {"away_team": "", "home_team": "", "innings": []} if parsing fails.
    """
    soup = BeautifulSoup(html, "html.parser")

    away_team = ""
    home_team = ""
    innings = []

    # Each inning lives in its own <div class="card table-responsive">
    # with the inning label in <div class="card-header">. We match BOTH
    # classes to avoid matching the outer page-wrapper <div class="card p-1">,
    # which would otherwise descend into the first inning and duplicate it.
    inning_cards = soup.select("div.card.table-responsive")

    for card in inning_cards:
        header = card.find("div", class_="card-header")
        if not header:
            continue

        header_text = header.get_text(strip=True)
        m = PBP_INNING_RE.search(header_text)
        if not m:
            continue  # not an inning card

        inning_num = int(m.group(1))

        table = card.find("table", class_="table")
        if not table:
            continue

        # --- Team names from thead (only needed once, but harmless to re-read) ---
        thead = table.find("thead")
        if thead and not (away_team and home_team):
            header_cells = thead.find_all("th")
            if len(header_cells) >= 3:
                # Prefer <img alt="..."> if present, else fall back to cell text
                def team_name_from_cell(cell):
                    img = cell.find("img")
                    if img and img.get("alt"):
                        return img["alt"].strip()
                    return cell.get_text(strip=True)

                away_team = team_name_from_cell(header_cells[0])
                home_team = team_name_from_cell(header_cells[2])

        # --- Body rows = all the at-bat / event rows for this inning ---
        tbody = table.find("tbody")
        if not tbody:
            continue

        body_rows = []
        for tr in tbody.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            cell_texts = [c.get_text(strip=True) for c in cells]
            body_rows.append(cell_texts)

        top_events    = _parse_pbp_events(body_rows, "away")
        bottom_events = _parse_pbp_events(body_rows, "home")

        # --- Footer = line-score per half ---
        top_line    = {}
        bottom_line = {}
        tfoot = table.find("tfoot")
        if tfoot:
            foot_row = tfoot.find("tr")
            if foot_row:
                foot_cells = foot_row.find_all(["td", "th"])
                if len(foot_cells) >= 3:
                    top_line    = _parse_linescore_cell(foot_cells[0].get_text(strip=True))
                    bottom_line = _parse_linescore_cell(foot_cells[2].get_text(strip=True))

        innings.append({
            "inning": inning_num,
            "top":    {"events": top_events,    "line": top_line},
            "bottom": {"events": bottom_events, "line": bottom_line},
        })

    # Sort by inning number just in case the page order is ever off
    innings.sort(key=lambda x: x["inning"])

    return {
        "away_team": away_team,
        "home_team": home_team,
        "innings":   innings,
    }


# ===========================================================================
# Opponent Pitchers Parser
# ===========================================================================
#
# On the /individual_stats page, there are typically TWO "Pitching" cards:
# one for MSU and one for the opposing team. The existing parser grabs only
# the MSU one. These helpers grab the opponent's table so we know which
# opposing pitchers threw in each game, in order of appearance (starter first).
#
# We need this for the Splits Selector — without the starter's name, we can't
# attribute PAs to a pitcher (since "X to p for Y" messages only fire on
# substitutions, never for the starter).


def find_opponent_pitching_table(html, team_name):
    """
    Find the opponent's pitching table on an /individual_stats page.

    Walks every "Pitching" card on the page and returns the first one whose
    header does NOT contain our team name. Returns None if nothing matches
    (e.g. a blowout where only one team's table rendered).

    Args:
        html: Full page HTML string.
        team_name: Our team name (e.g. "Mississippi St.") — used to skip
            MSU's own card.

    Returns:
        BeautifulSoup <table> element or None.
    """
    soup = BeautifulSoup(html, "html.parser")
    for header in soup.find_all("div", class_="card-header"):
        header_text = header.get_text(strip=True)
        if "Pitching" not in header_text:
            continue
        if team_name in header_text:
            continue  # skip MSU's card
        card = header.parent
        if card:
            table = card.find("table")
            if table:
                return table
    return None


def parse_opponent_pitchers(html, team_name):
    """
    Extract the opposing team's pitcher list in order of appearance.

    The NCAA individual_stats pitching table lists pitchers in the order they
    appeared in the game (starter first, then relievers in the order they
    entered). We parse the Name column directly and preserve that row order.

    Args:
        html: Full page HTML string of the /individual_stats page.
        team_name: Our team name (to skip MSU's own table).

    Returns:
        List of opposing pitcher name strings in appearance order. Empty list
        if the opponent's pitching table couldn't be found.
    """
    table = find_opponent_pitching_table(html, team_name)
    if not table:
        return []

    _, rows = parse_individual_table(table)
    pitchers = []
    for r in rows:
        if r.get("_is_totals"):
            continue
        name = r.get("Name", "").strip()
        if name:
            pitchers.append(name)
    return pitchers


# ===========================================================================
# Roster Scraper (for pitcher handedness)
# ===========================================================================
#
# The /teams/{id}/roster page on stats.ncaa.org lists every player with their
# position, bats, and throws. We only care about pitchers and their "Throws"
# value. We scrape each opponent once per season and cache the result.
#
# Expected page structure (inside a Bootstrap card table):
#   <table class="table">
#     <thead>
#       <tr><th>#</th><th>Name</th><th>Cl</th><th>Position</th>
#           <th>B</th><th>T</th><th>Ht</th><th>...</th></tr>
#     </thead>
#     <tbody>
#       <tr><td>1</td><td>Jon Smith</td><td>Sr</td><td>P</td>
#           <td>R</td><td>R</td>...</tr>
#     </tbody>
#   </table>
#
# Position codes we treat as pitchers: anything containing "P" as a whole
# token ("P", "P/OF", "RHP", "LHP", "SP", "RP"). Two-way players (like
# Shohei-style "P/DH") are also included.


# Matches a token that means "pitcher" — standalone P, or the pitching
# abbreviations RHP/LHP/SP/RP. Case-insensitive.
PITCHER_POS_RE = re.compile(r"\b(P|RHP|LHP|SP|RP)\b", re.IGNORECASE)


def parse_roster(html):
    """
    Parse an NCAA team roster page, extracting EVERY player's name along
    with both their batting side and throwing hand (not just declared
    pitchers).

    Why every player? In college baseball, position players (infielders,
    outfielders, utility) routinely come in to pitch in blowout games and
    appear in the play-by-play as relievers. If we only kept declared
    pitchers, those events would miss handedness lookups. Since every row
    on the roster has both Bats and Throws values, we keep them all and
    let the downstream lookup disambiguate by name.

    Strategy:
      1. Find the first <table class="table"> that has a column header
         containing "Throws" and actual tbody rows (there are usually two
         tables; the first is an empty placeholder).
      2. Walk every row (no position filter).
      3. Extract the name, batting side ("B" column), and throwing hand
         ("T" or "Throws" column) from each row.

    Args:
        html: Full page HTML string of the /teams/{id}/roster page.

    Returns:
        Dict mapping player name -> {"throws": "R/L/S/", "bats": "R/L/S/"}:
          { "Jon Smith": {"throws": "R", "bats": "L"}, ... }
        Returns {} if parsing fails.
    """
    soup = BeautifulSoup(html, "html.parser")

    # --- Locate the roster table ---
    # stats.ncaa.org renders the roster inside a table-responsive card. We
    # look for a table whose header row mentions "Throws" (or has separate
    # Bats/Throws columns labelled "B"/"T"). This avoids accidentally picking
    # up a coaching staff table elsewhere on the page.
    #
    # NOTE: NCAA roster pages contain TWO tables with identical headers: the
    # first is an empty placeholder (header-only, no tbody rows) used as a
    # structural hook by their DataTables JavaScript, and the second is the
    # real roster. We must walk past the empty one, so we require the table
    # to have at least one <tbody><tr>.
    target_table = None
    table_headers = []
    for table in soup.find_all("table"):
        thead = table.find("thead")
        if not thead:
            continue
        header_row = thead.find("tr")
        if not header_row:
            continue
        headers = [
            th.get_text(strip=True)
            for th in header_row.find_all(["th", "td"])
        ]
        headers_lower = [h.lower() for h in headers]
        # Accept either a "Throws" column or a "T" column paired with "B"
        has_throws = any(
            h == "throws" or h == "t" for h in headers_lower
        )
        has_position = any(
            "position" in h or h == "pos" for h in headers_lower
        )
        if not (has_throws and has_position):
            continue

        # Reject header-only placeholder tables. The real roster table has
        # body rows; the NCAA page's first (hidden) copy does not.
        tbody = table.find("tbody")
        if not tbody or not tbody.find("tr"):
            continue

        target_table = table
        table_headers = headers
        break

    if not target_table:
        return {}

    # Build a header-index map so we can find "Name", "Position", "Throws"
    # by label (order is not always consistent between divisions / years).
    header_idx = {h.lower(): i for i, h in enumerate(table_headers)}

    def col(name_options, row_cells):
        """Return cell text for the first matching header, or ''."""
        for opt in name_options:
            if opt in header_idx:
                idx = header_idx[opt]
                if idx < len(row_cells):
                    return row_cells[idx].get_text(strip=True)
        return ""

    players = {}
    tbody = target_table.find("tbody")
    if not tbody:
        return {}

    for tr in tbody.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) < 3:
            continue

        # Keep EVERY player, not just declared pitchers. Position players
        # sometimes pitch in relief and still need their hand looked up.
        name = col(["name", "player"], cells)
        if not name:
            continue

        # ---- Normalise the Bats and Throws cells to single-letter codes ----
        # NCAA roster pages use full words "LEFT" / "RIGHT" / "SWITCH"
        # (and the Spanish-style "BOTH" appears occasionally for switch
        # hitters/throwers). Single-letter codes "L" / "R" / "S" also show
        # up in some divisions. Anything we don't recognise becomes "".
        def _norm_hand(raw):
            r = raw.upper().strip()
            if r in ("R", "RIGHT"):
                return "R"
            if r in ("L", "LEFT"):
                return "L"
            if r in ("S", "B", "BOTH", "SWITCH"):
                return "S"
            return ""

        throws = _norm_hand(col(["throws", "t"], cells))
        bats   = _norm_hand(col(["bats", "b"], cells))

        players[name] = {"throws": throws, "bats": bats}

    return players


# Roster cache schema version. Bump this whenever the per-player record
# shape changes — load_roster_cache() returns an empty dict on a version
# mismatch, which forces scrape-stats.py to refetch every roster the next
# time it runs.
#
# Version history:
#   1 — players: { name: "R" }                       (throws only)
#   2 — players: { name: {"throws": "R", "bats": "L"} }  (both columns)
ROSTER_CACHE_VERSION = 2


def load_roster_cache(roster_cache_path):
    """
    Load the roster cache from disk, returning {} when:
      - the file doesn't exist
      - the file fails to parse
      - the on-disk schema version doesn't match ROSTER_CACHE_VERSION

    The version check ensures that bumping ROSTER_CACHE_VERSION (because
    we changed the per-player record shape) automatically forces a fresh
    scrape of every opponent roster, so old cached entries don't poison
    the new format.

    Structure on disk:
        {
          "version": 2,
          "teams": {
            "614619": {
              "name": "Hofstra",
              "players": {"Jon Smith": {"throws": "R", "bats": "L"}, ...}
            },
            ...
          }
        }
    """
    if not roster_cache_path.exists():
        return {}
    try:
        with open(roster_cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("version") != ROSTER_CACHE_VERSION:
            print(
                f"  Roster cache version mismatch "
                f"(found {data.get('version')!r}, expected {ROSTER_CACHE_VERSION}). "
                f"All opponent rosters will be refetched."
            )
            return {}
        return data.get("teams", {})
    except (json.JSONDecodeError, OSError):
        return {}


def save_roster_cache(roster_cache_path, teams):
    """Save the roster cache to disk, tagged with the current schema version."""
    roster_cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(roster_cache_path, "w", encoding="utf-8") as f:
        json.dump({"version": ROSTER_CACHE_VERSION, "teams": teams}, f, indent=2)


# ===========================================================================
# Safe Integer/Float Parsing
# ===========================================================================

def safe_int(val, default=0):
    """
    Safely parse a string to int. Returns default if empty or unparseable.

    Args:
        val: String value to parse.
        default: Value to return on failure.

    Returns:
        Integer value or default.
    """
    if val is None:
        return default
    val = str(val).strip()
    if not val or val == "-":
        return default
    try:
        return int(val)
    except ValueError:
        return default


def safe_float(val, default=0.0):
    """
    Safely parse a string to float. Returns default if empty or unparseable.

    Args:
        val: String value to parse.
        default: Value to return on failure.

    Returns:
        Float value or default.
    """
    if val is None:
        return default
    val = str(val).strip()
    if not val or val == "-":
        return default
    try:
        return float(val)
    except ValueError:
        return default


# ===========================================================================
# Stat Formatting Helpers
# ===========================================================================

def fmt_avg(numerator, denominator):
    """
    Format a batting average / rate stat as ".xxx" (no leading zero).
    Returns ".000" if denominator is 0.

    Args:
        numerator: int or float
        denominator: int or float

    Returns:
        String like ".325" or ".000".
    """
    if denominator == 0:
        return ".000"
    val = numerator / denominator
    return f"{val:.3f}"[1:] if val < 1.0 else f"{val:.3f}"


def fmt_pct(numerator, denominator):
    """
    Format a percentage stat as "xx.xx%".
    Returns "0.00%" if denominator is 0.

    Args:
        numerator: int or float
        denominator: int or float

    Returns:
        String like "14.07%" or "0.00%".
    """
    if denominator == 0:
        return "0.00%"
    val = (numerator / denominator) * 100
    return f"{val:.2f}%"


def fmt_rate(numerator, denominator):
    """
    Format a rate stat as "x.xx".
    Returns "0.00" if denominator is 0.

    Args:
        numerator: int or float
        denominator: int or float

    Returns:
        String like "9.45" or "0.00".
    """
    if denominator == 0:
        return "0.00"
    val = numerator / denominator
    return f"{val:.2f}"


# ===========================================================================
# Aggregation — Individual Stats
# ===========================================================================

def aggregate_individual_stats(all_game_data, stat_type, game_indices=None):
    """
    Aggregate raw counting stats across games for each player.

    Sums every numeric column per player across the specified games.
    Also tracks games played (GP), games started (GS), and for pitchers,
    properly handles IP conversion via thirds.

    Args:
        all_game_data: List of dicts per game. Each dict has:
            - 'headers': list of column names
            - 'players': list of player row dicts (from parse_individual_table)
        stat_type: "hitting", "pitching", or "fielding"
        game_indices: Optional set of game indices to include (for SEC filter).
                      If None, all games are included.

    Returns:
        Dict with:
          - 'players': dict mapping player name to aggregated stats dict
          - 'team_totals': aggregated team totals dict
          - 'all_headers': union of all header names seen
    """
    # Columns that are not summed (they are identifiers)
    skip_cols = {"Name", "P", "_is_totals", "_row_index"}

    # Collect all stat headers across games
    all_headers = []
    seen = set()
    for idx, game in enumerate(all_game_data):
        if game_indices is not None and idx not in game_indices:
            continue
        for h in game["headers"]:
            if h not in seen and h not in skip_cols:
                seen.add(h)
                all_headers.append(h)

    # Per-player aggregation
    player_agg = {}   # name -> {"stats": {col: value}, "gp": int, "gs": int, "positions": list}
    team_agg = {}     # col -> summed value

    for idx, game in enumerate(all_game_data):
        if game_indices is not None and idx not in game_indices:
            continue

        players = game["players"]
        # Separate non-totals players from the totals row
        regular_players = [p for p in players if not p.get("_is_totals", False)]
        totals_row = next((p for p in players if p.get("_is_totals", False)), None)

        # Aggregate team totals row
        if totals_row:
            for col in all_headers:
                val_str = totals_row.get(col, "0")
                if col == "IP":
                    thirds = ip_to_thirds(val_str)
                    team_agg[col] = team_agg.get(col, 0) + thirds
                else:
                    team_agg[col] = team_agg.get(col, 0) + safe_int(val_str)

        # Aggregate individual players
        for p in regular_players:
            name = p.get("Name", "")
            if not name:
                continue

            pos = p.get("P", "")

            if name not in player_agg:
                player_agg[name] = {
                    "stats": {},
                    "gp": 0,
                    "gs": 0,
                    "positions": [],
                }

            player_agg[name]["gp"] += 1

            # Track position for this game
            if pos:
                player_agg[name]["positions"].append(pos)

            # Determine if this player started this game:
            # For hitters: started if they are in the first 9 non-totals rows
            # For pitchers: started if they are the FIRST pitcher listed
            # For fielding: started if in first 9 rows (same as hitting)
            if stat_type == "pitching":
                if regular_players and regular_players[0].get("Name") == name:
                    player_agg[name]["gs"] += 1
            else:
                # Hitters/fielders: started if in batting order positions 1-9
                row_index = p.get("_row_index", 999)
                if row_index < 9:
                    player_agg[name]["gs"] += 1

            # Sum counting stats
            for col in all_headers:
                val_str = p.get(col, "0")
                if col == "IP":
                    thirds = ip_to_thirds(val_str)
                    player_agg[name]["stats"][col] = player_agg[name]["stats"].get(col, 0) + thirds
                else:
                    player_agg[name]["stats"][col] = (
                        player_agg[name]["stats"].get(col, 0) + safe_int(val_str)
                    )

    return {
        "players": player_agg,
        "team_totals": team_agg,
        "all_headers": all_headers,
    }


# ===========================================================================
# Derived Stats — Hitting
# ===========================================================================

# The exact output column order for the hitting stats JSON
HITTING_COLUMNS = [
    "Player", "PA", "BB%", "K%", "AVG", "OBP", "SLG", "ISO", "BABIP",
    "GP-GS", "AB", "R", "H", "2B", "3B", "HR", "RBI", "TB", "BB",
    "HBP", "SO", "GIDP", "SF", "SH", "SB-ATT", "PO", "A", "E", "FLD%",
]


def calculate_hitting_stats(hitting_agg, fielding_agg):
    """
    Calculate derived hitting stats for all players and build the output rows.

    Merges hitting + fielding data by player name to include PO, A, E, FLD%.

    Args:
        hitting_agg: Output of aggregate_individual_stats() for hitting.
        fielding_agg: Output of aggregate_individual_stats() for fielding.

    Returns:
        Tuple of (player_rows, summary_rows) where each is a list of lists
        (each inner list has values in HITTING_COLUMNS order).
    """
    player_rows = []

    for name, data in hitting_agg["players"].items():
        s = data["stats"]
        gp = data["gp"]
        gs = data["gs"]

        # Raw counting stats from the hitting table
        ab = s.get("AB", 0)
        r = s.get("R", 0)
        h = s.get("H", 0)
        doubles = s.get("2B", 0)
        triples = s.get("3B", 0)
        hr = s.get("HR", 0)
        rbi = s.get("RBI", 0)
        tb = s.get("TB", 0)
        bb = s.get("BB", 0)
        hbp = s.get("HBP", 0)
        sf = s.get("SF", 0)
        sh = s.get("SH", 0)
        k = s.get("K", 0)
        gdp = s.get("OPPDP", 0)  # NCAA labels grounded-into-double-play as "OPPDP"
        sb = s.get("SB", 0)
        cs = s.get("CS", 0)

        # Fielding stats (merged by player name)
        f_data = fielding_agg["players"].get(name, {})
        f_stats = f_data.get("stats", {}) if f_data else {}
        po = f_stats.get("PO", 0)
        a = f_stats.get("A", 0)
        e = f_stats.get("E", 0)

        # Derived stats
        # PA = AB + BB + HBP + SF (sacrifice hits are NOT counted as plate appearances)
        pa = ab + bb + hbp + sf
        if pa == 0:
            continue  # Skip players with no plate appearances

        bb_pct = fmt_pct(bb, pa)
        k_pct = fmt_pct(k, pa)
        avg = fmt_avg(h, ab)
        obp_denom = ab + bb + hbp + sf
        obp = fmt_avg(h + bb + hbp, obp_denom)
        slg = fmt_avg(tb, ab)
        iso = fmt_avg(tb - h, ab)

        babip_denom = ab - k - hr + sf
        babip = fmt_avg(h - hr, babip_denom) if babip_denom > 0 else ".000"

        gp_gs = f"{gp}-{gs}"
        sb_att = f"{sb}-{sb + cs}"

        fld_denom = po + a + e
        fld_pct = fmt_avg(po + a, fld_denom) if fld_denom > 0 else ".000"

        row = [
            name, str(pa), bb_pct, k_pct, avg, obp, slg, iso, babip,
            gp_gs, str(ab), str(r), str(h), str(doubles), str(triples),
            str(hr), str(rbi), str(tb), str(bb), str(hbp), str(k),
            str(gdp), str(sf), str(sh), sb_att, str(po), str(a), str(e), fld_pct,
        ]
        player_rows.append(row)

    # Sort by PA descending
    player_rows.sort(key=lambda row: int(row[1]), reverse=True)

    # --- Team totals row ---
    ts = hitting_agg["team_totals"]
    ft = fielding_agg["team_totals"]

    t_ab = ts.get("AB", 0)
    t_r = ts.get("R", 0)
    t_h = ts.get("H", 0)
    t_2b = ts.get("2B", 0)
    t_3b = ts.get("3B", 0)
    t_hr = ts.get("HR", 0)
    t_rbi = ts.get("RBI", 0)
    t_tb = ts.get("TB", 0)
    t_bb = ts.get("BB", 0)
    t_hbp = ts.get("HBP", 0)
    t_sf = ts.get("SF", 0)
    t_sh = ts.get("SH", 0)
    t_k = ts.get("K", 0)
    t_gdp = ts.get("OPPDP", 0)  # NCAA labels grounded-into-double-play as "OPPDP"
    t_sb = ts.get("SB", 0)
    t_cs = ts.get("CS", 0)
    t_po = ft.get("PO", 0)
    t_a = ft.get("A", 0)
    t_e = ft.get("E", 0)

    # PA = AB + BB + HBP + SF (sacrifice hits are NOT counted as plate appearances)
    t_pa = t_ab + t_bb + t_hbp + t_sf
    t_bb_pct = fmt_pct(t_bb, t_pa)
    t_k_pct = fmt_pct(t_k, t_pa)
    t_avg = fmt_avg(t_h, t_ab)
    t_obp_denom = t_ab + t_bb + t_hbp + t_sf
    t_obp = fmt_avg(t_h + t_bb + t_hbp, t_obp_denom)
    t_slg = fmt_avg(t_tb, t_ab)
    t_iso = fmt_avg(t_tb - t_h, t_ab)
    t_babip_denom = t_ab - t_k - t_hr + t_sf
    t_babip = fmt_avg(t_h - t_hr, t_babip_denom) if t_babip_denom > 0 else ".000"

    # GP-GS for totals: count of games
    num_games = len(hitting_agg["players"])  # not quite right, use gamesScraped later
    t_gp_gs = "-"
    t_sb_att = f"{t_sb}-{t_sb + t_cs}"
    t_fld_denom = t_po + t_a + t_e
    t_fld_pct = fmt_avg(t_po + t_a, t_fld_denom) if t_fld_denom > 0 else ".000"

    totals_row = [
        "Totals", str(t_pa), t_bb_pct, t_k_pct, t_avg, t_obp, t_slg, t_iso, t_babip,
        t_gp_gs, str(t_ab), str(t_r), str(t_h), str(t_2b), str(t_3b),
        str(t_hr), str(t_rbi), str(t_tb), str(t_bb), str(t_hbp), str(t_k),
        str(t_gdp), str(t_sf), str(t_sh), t_sb_att, str(t_po), str(t_a), str(t_e), t_fld_pct,
    ]

    return player_rows, [totals_row]


# ===========================================================================
# Derived Stats — Pitching
# ===========================================================================

# The exact output column order for the pitching stats JSON
PITCHING_COLUMNS = [
    "Player", "IP", "PA", "K/9", "BB/9", "HR/9", "K%", "BB%", "K%-BB%",
    "WHIP", "ERA", "FIP", "LOB%", "BABIP",
    "APP-GS", "W-L", "SV", "H", "R", "ER", "BB", "SO", "2B", "3B", "HR",
    "AB", "b/avg", "WP", "HBP", "BK", "SFA", "SHA", "SBA", "CSB", "SBA%",
]


def calculate_pitching_stats(pitching_agg, decisions_per_game, cg_tracker, sho_tracker):
    """
    Calculate derived pitching stats for all players and build the output rows.

    Args:
        pitching_agg: Output of aggregate_individual_stats() for pitching.
        decisions_per_game: List of decision dicts per game (from parse_box_score_decisions).
        cg_tracker: Dict mapping player name to complete game count.
        sho_tracker: Dict mapping player name to shutout count.

    Returns:
        Tuple of (player_rows, summary_rows) where each is a list of lists
        (each inner list has values in PITCHING_COLUMNS order).
    """
    # Tally W/L/SV per pitcher from decisions_per_game
    wins = defaultdict(int)
    losses = defaultdict(int)
    saves = defaultdict(int)
    all_pitcher_names = set(pitching_agg["players"].keys())

    for decisions in decisions_per_game:
        if not decisions:
            continue
        for dec_type, dec_name in [("win", decisions.get("win")),
                                    ("loss", decisions.get("loss")),
                                    ("save", decisions.get("save"))]:
            if not dec_name:
                continue
            # Match against our roster
            matched = _match_name(dec_name, all_pitcher_names)
            if matched:
                if dec_type == "win":
                    wins[matched] += 1
                elif dec_type == "loss":
                    losses[matched] += 1
                elif dec_type == "save":
                    saves[matched] += 1

    player_rows = []

    for name, data in pitching_agg["players"].items():
        s = data["stats"]
        gp = data["gp"]
        gs = data["gs"]

        # IP is stored as total thirds
        ip_thirds = s.get("IP", 0)
        ip_display = thirds_to_ip(ip_thirds)
        ip_float = thirds_to_float(ip_thirds)

        if ip_float == 0 and gp == 0:
            continue  # Skip pitchers with no appearances

        # Raw counting stats from the pitching table
        h = s.get("H", 0)
        r = s.get("R", 0)
        er = s.get("ER", 0)
        bb = s.get("BB", 0)
        so = s.get("SO", 0)
        bf = s.get("BF", 0)
        doubles_a = s.get("2B-A", 0)
        triples_a = s.get("3B-A", 0)
        hr_a = s.get("HR-A", 0)
        wp = s.get("WP", 0)
        hb = s.get("HB", 0)
        ibb = s.get("IBB", 0)
        bk = s.get("Bk", 0)
        sha = s.get("SHA", 0)
        sfa = s.get("SFA", 0)

        # Decisions
        w = wins.get(name, 0)
        l = losses.get(name, 0)
        sv = saves.get(name, 0)
        cg = cg_tracker.get(name, 0)
        sho = sho_tracker.get(name, 0)

        # Derived stats
        #
        # NCAA's "BF" (batters faced) column counts every plate appearance
        # including sacrifice bunts, but the standard sabermetric PA
        # definition EXCLUDES sac bunts: PA = AB + BB + HBP + SF. So we
        # compute PA as BF - SHA and use that for the displayed column and
        # as the denominator for K% and BB%.
        pa = bf - sha
        k9 = fmt_rate(so * 9, ip_float) if ip_float > 0 else "0.00"
        bb9 = fmt_rate(bb * 9, ip_float) if ip_float > 0 else "0.00"
        hr9 = fmt_rate(hr_a * 9, ip_float) if ip_float > 0 else "0.00"

        # K% and BB% use PA (not BF) as the denominator so sac bunts are
        # excluded, matching the sabermetric convention.
        k_pct_raw = (so / pa) * 100 if pa > 0 else 0.0
        bb_pct_raw = (bb / pa) * 100 if pa > 0 else 0.0
        k_pct = f"{k_pct_raw:.2f}%"
        bb_pct = f"{bb_pct_raw:.2f}%"
        k_minus_bb = f"{k_pct_raw - bb_pct_raw:.2f}%"

        whip = fmt_rate(bb + h, ip_float) if ip_float > 0 else "0.00"
        era = fmt_rate(er * 9, ip_float) if ip_float > 0 else "0.00"

        # FIP = ((13*HR) + (3*(BB+HBP)) - (2*SO)) / IP + 3.2
        if ip_float > 0:
            fip_val = ((13 * hr_a) + (3 * (bb + hb)) - (2 * so)) / ip_float + 3.2
            fip = f"{fip_val:.2f}"
        else:
            fip = "0.00"

        # LOB% = (H+BB+HBP-R) / (H+BB+HBP-(1.4*HR))
        lob_denom = h + bb + hb - (1.4 * hr_a)
        if lob_denom > 0:
            lob_val = ((h + bb + hb - r) / lob_denom) * 100
            lob_pct = f"{lob_val:.1f}%"
        else:
            lob_pct = "0.0%"

        # BABIP = (H - HR) / (AB_against - SO - HR + SFA)
        ab_against = bf - bb - hb - sfa - sha
        babip_denom = ab_against - so - hr_a + sfa
        babip = fmt_avg(h - hr_a, babip_denom) if babip_denom > 0 else ".000"

        app_gs = f"{gp}-{gs}"
        w_l = f"{w}-{l}"

        # Opponent batting average (b/avg) = H / AB
        b_avg = fmt_avg(h, ab_against) if ab_against > 0 else ".000"

        # SBA stats
        sba = s.get("SBA", 0)
        csb = s.get("CSB", 0)
        sba_pct = fmt_pct(sba, sba + csb) if (sba + csb) > 0 else ".000"

        # Row in PITCHING_COLUMNS order
        row = [
            name, ip_display, str(pa), k9, bb9, hr9, k_pct, bb_pct, k_minus_bb,
            whip, era, fip, lob_pct, babip,
            app_gs, w_l, str(sv), str(h), str(r), str(er), str(bb), str(so),
            str(doubles_a), str(triples_a), str(hr_a),
            str(ab_against), b_avg, str(wp), str(hb), str(bk), str(sfa), str(sha),
            str(sba), str(csb), sba_pct,
        ]
        player_rows.append(row)

    # Sort by IP descending (use ip_thirds stored in stats for proper ordering)
    def sort_ip(row):
        """Extract IP thirds from player aggregation for sorting."""
        name = row[0]
        data = pitching_agg["players"].get(name, {})
        return data.get("stats", {}).get("IP", 0)
    player_rows.sort(key=sort_ip, reverse=True)

    # --- Team totals row ---
    ts = pitching_agg["team_totals"]
    t_ip_thirds = ts.get("IP", 0)
    t_ip_display = thirds_to_ip(t_ip_thirds)
    t_ip_float = thirds_to_float(t_ip_thirds)

    t_h = ts.get("H", 0)
    t_r = ts.get("R", 0)
    t_er = ts.get("ER", 0)
    t_bb = ts.get("BB", 0)
    t_so = ts.get("SO", 0)
    t_bf = ts.get("BF", 0)
    t_2ba = ts.get("2B-A", 0)
    t_3ba = ts.get("3B-A", 0)
    t_hra = ts.get("HR-A", 0)
    t_wp = ts.get("WP", 0)
    t_hb = ts.get("HB", 0)
    t_bk = ts.get("Bk", 0)
    t_sha = ts.get("SHA", 0)
    t_sfa = ts.get("SFA", 0)

    # PA excludes sac bunts: PA = BF - SHA. K% / BB% use PA (not BF) as
    # the denominator so sac bunts are excluded, matching the sabermetric
    # convention (mirrors the individual-pitcher calculation above).
    t_pa = t_bf - t_sha
    t_k9 = fmt_rate(t_so * 9, t_ip_float) if t_ip_float > 0 else "0.00"
    t_bb9 = fmt_rate(t_bb * 9, t_ip_float) if t_ip_float > 0 else "0.00"
    t_hr9 = fmt_rate(t_hra * 9, t_ip_float) if t_ip_float > 0 else "0.00"

    t_k_pct_raw = (t_so / t_pa) * 100 if t_pa > 0 else 0.0
    t_bb_pct_raw = (t_bb / t_pa) * 100 if t_pa > 0 else 0.0
    t_k_pct = f"{t_k_pct_raw:.2f}%"
    t_bb_pct = f"{t_bb_pct_raw:.2f}%"
    t_k_minus_bb = f"{t_k_pct_raw - t_bb_pct_raw:.2f}%"

    t_whip = fmt_rate(t_bb + t_h, t_ip_float) if t_ip_float > 0 else "0.00"
    t_era = fmt_rate(t_er * 9, t_ip_float) if t_ip_float > 0 else "0.00"

    # FIP
    if t_ip_float > 0:
        t_fip_val = ((13 * t_hra) + (3 * (t_bb + t_hb)) - (2 * t_so)) / t_ip_float + 3.2
        t_fip = f"{t_fip_val:.2f}"
    else:
        t_fip = "0.00"

    # LOB%
    t_lob_denom = t_h + t_bb + t_hb - (1.4 * t_hra)
    if t_lob_denom > 0:
        t_lob_val = ((t_h + t_bb + t_hb - t_r) / t_lob_denom) * 100
        t_lob_pct = f"{t_lob_val:.1f}%"
    else:
        t_lob_pct = "0.0%"

    t_ab_against = t_bf - t_bb - t_hb - t_sfa - t_sha
    t_babip_denom = t_ab_against - t_so - t_hra + t_sfa
    t_babip = fmt_avg(t_h - t_hra, t_babip_denom) if t_babip_denom > 0 else ".000"

    t_w = sum(wins.values())
    t_l = sum(losses.values())
    t_sv = sum(saves.values())

    t_b_avg = fmt_avg(t_h, t_ab_against) if t_ab_against > 0 else ".000"

    t_sba = ts.get("SBA", 0)
    t_csb = ts.get("CSB", 0)
    t_sba_pct = fmt_pct(t_sba, t_sba + t_csb) if (t_sba + t_csb) > 0 else ".000"

    totals_row = [
        "Totals", t_ip_display, str(t_pa), t_k9, t_bb9, t_hr9, t_k_pct, t_bb_pct, t_k_minus_bb,
        t_whip, t_era, t_fip, t_lob_pct, t_babip,
        "-", f"{t_w}-{t_l}", str(t_sv), str(t_h), str(t_r), str(t_er), str(t_bb), str(t_so),
        str(t_2ba), str(t_3ba), str(t_hra),
        str(t_ab_against), t_b_avg, str(t_wp), str(t_hb), str(t_bk), str(t_sfa), str(t_sha),
        str(t_sba), str(t_csb), t_sba_pct,
    ]

    return player_rows, [totals_row]


# ===========================================================================
# Derived Stats — Fielding
# ===========================================================================

# The exact output column order for the fielding stats JSON
FIELDING_COLUMNS = [
    "Player", "C", "GP", "INN", "TC", "PO", "A", "E", "CI", "FLD%", "DP", "SBA",
]


def calculate_fielding_stats(fielding_agg):
    """
    Calculate fielding stats for all players and build the output rows.

    Args:
        fielding_agg: Output of aggregate_individual_stats() for fielding.

    Returns:
        Tuple of (player_rows, summary_rows) where each is a list of lists
        (each inner list has values in FIELDING_COLUMNS order).
    """
    player_rows = []

    for name, data in fielding_agg["players"].items():
        s = data["stats"]
        gp = data["gp"]
        positions = data.get("positions", [])

        # Primary position = most frequently played
        if positions:
            pos_counts = defaultdict(int)
            for p in positions:
                pos_counts[p] += 1
            primary_pos = max(pos_counts, key=pos_counts.get)
        else:
            primary_pos = "-"

        po = s.get("PO", 0)
        a = s.get("A", 0)
        tc = s.get("TC", 0)
        e = s.get("E", 0)
        ci = s.get("CI", 0)
        idp = s.get("IDP", 0)
        sba = s.get("SBA", 0)

        # If TC wasn't in the table, calculate it
        if tc == 0 and (po + a + e) > 0:
            tc = po + a + e

        fld_denom = po + a + e
        fld_pct = fmt_avg(po + a, fld_denom) if fld_denom > 0 else ".000"

        if gp == 0:
            continue

        row = [
            name, primary_pos, str(gp), "-",  # INN = "-" (not available from NCAA)
            str(tc), str(po), str(a), str(e), str(ci),
            fld_pct, str(idp), str(sba),
        ]
        player_rows.append(row)

    # Sort by GP descending, then by name
    player_rows.sort(key=lambda row: (-int(row[2]), row[0]))

    # --- Team totals row ---
    ft = fielding_agg["team_totals"]
    t_po = ft.get("PO", 0)
    t_a = ft.get("A", 0)
    t_tc = ft.get("TC", 0)
    t_e = ft.get("E", 0)
    t_ci = ft.get("CI", 0)
    t_idp = ft.get("IDP", 0)
    t_sba = ft.get("SBA", 0)

    if t_tc == 0 and (t_po + t_a + t_e) > 0:
        t_tc = t_po + t_a + t_e

    t_fld_denom = t_po + t_a + t_e
    t_fld_pct = fmt_avg(t_po + t_a, t_fld_denom) if t_fld_denom > 0 else ".000"

    totals_row = [
        "Totals", "-", "-", "-",
        str(t_tc), str(t_po), str(t_a), str(t_e), str(t_ci),
        t_fld_pct, str(t_idp), str(t_sba),
    ]

    return player_rows, [totals_row]


# ===========================================================================
# Name Matching Helper
# ===========================================================================

def _match_name(target_name, roster_names):
    """
    Match a player name from box score decisions against the roster.

    First tries exact match, then tries substring matching (the box score
    may abbreviate or format names slightly differently).

    Args:
        target_name: Name string from box score (e.g. "Stone, Duke").
        roster_names: Set of player name strings from our individual stats.

    Returns:
        Matched name from roster_names, or None if no match found.
    """
    if not target_name:
        return None

    target_clean = target_name.strip()

    # Exact match
    if target_clean in roster_names:
        return target_clean

    # Try case-insensitive exact match
    for rn in roster_names:
        if rn.lower() == target_clean.lower():
            return rn

    # Try substring: if the target last name appears at the start of a roster name
    # NCAA typically uses "Last, First" format
    target_last = target_clean.split(",")[0].strip().lower()
    for rn in roster_names:
        rn_last = rn.split(",")[0].strip().lower()
        if target_last == rn_last:
            return rn

    return None


# ===========================================================================
# Main
# ===========================================================================

def _save_cache(cache):
    """
    Persist the in-memory cache dict to CACHE_PATH.

    Called after each newly scraped game so that progress is preserved
    if the run is interrupted mid-way.

    Args:
        cache: Dict mapping contest ID strings to per-game raw data dicts.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump({"version": 1, "games": cache}, f)


def main():
    """
    Main entry point. Launches a browser, scrapes each game's individual stats,
    box score, and situational stats, aggregates everything, calculates derived
    metrics, and writes five JSON files.

    Pass --headless for CI environments (GitHub Actions). The NCAA site uses
    Akamai bot detection, so headless mode injects JavaScript to mask common
    fingerprinting signals.

    Pass --full to ignore the cache and re-scrape every game from scratch.
    Without --full, only games not already in the cache are fetched.
    """
    parser = argparse.ArgumentParser(
        description="Scrape MSU individual stats + situational splits from NCAA stats."
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run in headless mode (for CI). Uses stealth evasion to bypass bot detection.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Ignore the cache and re-scrape all games from scratch.",
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Starting NCAA combined stats scraper for {TEAM_NAME}")
    print(f"Team URL: {TEAM_URL}")
    print(f"Mode: {'headless (stealth)' if args.headless else 'headed (visible Chrome)'}")
    print()

    # ==============================================================
    # Load cache
    # ==============================================================
    cache = {}  # contest_id (str) -> per-game raw data dict

    if args.full:
        print("--full flag set: ignoring cache, re-scraping all games.")
    elif CACHE_PATH.exists():
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            cache_data = json.load(f)
            cache = cache_data.get("games", {})
        print(f"Cache loaded: {len(cache)} game(s) already scraped.")
    else:
        print("No cache found — starting fresh.")
    print()

    # Stealth JavaScript for headless mode — patches common bot-detection signals
    STEALTH_SCRIPT = """
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        window.chrome = { runtime: {} };
    """

    with sync_playwright() as pw:
        # --- Browser Setup ---
        if args.headless:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
                locale="en-US",
            )
            context.add_init_script(STEALTH_SCRIPT)
        else:
            browser = pw.chromium.launch(headless=False, channel="chrome")
            context = browser.new_context()

        page = context.new_page()

        # ==============================================================
        # Step 1: Load schedule, extract game info
        # ==============================================================
        print("Loading team schedule page...")
        page.goto(TEAM_URL, wait_until="networkidle", timeout=30000)
        games = extract_game_info(page)
        full_schedule = extract_full_schedule(page)
        print(f"Found {len(games)} completed games, {len(full_schedule)} total schedule entries.\n")

        if not games:
            print("ERROR: No completed games found. Check the team URL.")
            browser.close()
            return

        # Identify SEC game indices (based on full schedule order, used later)
        sec_game_indices = {i for i, g in enumerate(games) if g["isSEC"]}
        # Non-conference = all games NOT marked as SEC (All Teams minus SEC Only)
        non_sec_game_indices = set(range(len(games))) - sec_game_indices
        print(f"SEC games: {len(sec_game_indices)} of {len(games)}")
        for i in sorted(sec_game_indices):
            print(f"  - {games[i]['date']} vs {games[i]['opponent']}")
        print(f"Non-conference games: {len(non_sec_game_indices)} of {len(games)}")
        print()

        last_game = games[-1]

        # ==============================================================
        # Step 2: Identify which games still need scraping
        #
        # There are three kinds of work:
        #   (a) full scrape      — game is not in the cache at all (all 4 pages)
        #   (b) pbp backfill     — game is cached but predates play_by_play
        #                          support; only need to fetch /play_by_play
        #   (c) opp-pitchers     — game is cached but predates opponent_pitchers
        #     backfill             support; only need to re-fetch page 1 to
        #                          grab the opposing team's pitching table.
        #
        # Backfill (c) also patches in opponentTeamId from the schedule, which
        # was added at the same time as opponent_pitchers.
        # ==============================================================
        games_to_scrape = [g for g in games if g["contestId"] not in cache]
        games_needing_pbp = [
            g for g in games
            if g["contestId"] in cache
            and not cache[g["contestId"]].get("play_by_play", {}).get("innings")
        ]
        games_needing_opp_pitchers = [
            g for g in games
            if g["contestId"] in cache
            and not cache[g["contestId"]].get("opponent_pitchers")
        ]
        cached_count = len(games) - len(games_to_scrape)

        if (
            not games_to_scrape
            and not games_needing_pbp
            and not games_needing_opp_pitchers
        ):
            print(f"All {len(games)} games already cached — skipping browser scraping.\n")
        else:
            if games_to_scrape:
                print(
                    f"Full scrape: {len(games_to_scrape)} new game(s) "
                    f"({cached_count} already cached)."
                )
            if games_needing_pbp:
                print(
                    f"PBP backfill: {len(games_needing_pbp)} cached game(s) "
                    f"missing play-by-play data."
                )
            if games_needing_opp_pitchers:
                print(
                    f"Opp pitchers backfill: {len(games_needing_opp_pitchers)} "
                    f"cached game(s) missing opponent pitcher list."
                )
            print()

        # ==============================================================
        # Step 3: Scrape only new games (3 pages each)
        # ==============================================================
        for i, game in enumerate(games_to_scrape):
            cid = game["contestId"]
            label = (
                f"[{i + 1}/{len(games_to_scrape)}] "
                f"Scraping game {cid} ({game['date']} vs {game['opponent']})..."
            )
            print(label, end=" ")

            status_parts = []

            # Temporary holders for this game's data
            g_hitting  = {"headers": [], "players": []}
            g_pitching = {"headers": [], "players": []}
            g_fielding = {"headers": [], "players": []}
            g_decisions    = {"win": None, "loss": None, "save": None}
            g_opp_runs     = 0
            g_hit_splits   = ([], [])
            g_pitch_splits = ([], [])
            g_pitchers     = []
            g_opp_pitchers = []
            g_pbp          = {"away_team": "", "home_team": "", "innings": []}

            # ---- Page 1: Individual Stats ----
            try:
                url = f"https://stats.ncaa.org/contests/{cid}/individual_stats"
                page.goto(url, wait_until="networkidle", timeout=30000)
                html = page.content()

                hitting_table = find_team_tables(html, TEAM_NAME, "Hitting")
                if hitting_table:
                    headers, players = parse_individual_table(hitting_table)
                    g_hitting = {"headers": headers, "players": players}

                pitching_table = find_team_tables(html, TEAM_NAME, "Pitching")
                if pitching_table:
                    headers, players = parse_individual_table(pitching_table)
                    g_pitching = {"headers": headers, "players": players}
                    g_pitchers = [
                        p["Name"] for p in players
                        if not p.get("_is_totals", False) and p.get("Name")
                    ]

                fielding_table = find_team_tables(html, TEAM_NAME, "Fielding")
                if fielding_table:
                    headers, players = parse_individual_table(fielding_table)
                    g_fielding = {"headers": headers, "players": players}

                # Opponent pitchers (for per-PA pitcher attribution later).
                # Starter appears first because the NCAA table lists pitchers
                # in order of appearance.
                g_opp_pitchers = parse_opponent_pitchers(html, TEAM_NAME)

                status_parts.append("individual_stats OK")
            except Exception as e:
                status_parts.append(f"individual_stats FAILED ({e})")

            time.sleep(REQUEST_DELAY)

            # ---- Page 2: Box Score ----
            try:
                url = f"https://stats.ncaa.org/contests/{cid}/box_score"
                page.goto(url, wait_until="networkidle", timeout=30000)
                html = page.content()
                g_decisions = parse_box_score_decisions(html, TEAM_NAME)
                g_opp_runs  = parse_opponent_runs(html, TEAM_NAME)
                status_parts.append("box_score OK")
            except Exception as e:
                status_parts.append(f"box_score FAILED ({e})")

            time.sleep(REQUEST_DELAY)

            # ---- Page 3: Situational Stats ----
            try:
                url = f"https://stats.ncaa.org/contests/{cid}/situational_stats"
                page.goto(url, wait_until="networkidle", timeout=30000)
                html = page.content()

                hitting_table = find_team_tables(html, TEAM_NAME, "Hitting")
                if hitting_table:
                    cols, players = parse_situational_table(hitting_table)
                    if players:
                        g_hit_splits = (cols, players)

                pitching_table = find_team_tables(html, TEAM_NAME, "Pitching")
                if pitching_table:
                    cols, players = parse_situational_table(pitching_table)
                    if players:
                        g_pitch_splits = (cols, players)

                status_parts.append("situational_stats OK")
            except Exception as e:
                status_parts.append(f"situational_stats FAILED ({e})")

            time.sleep(REQUEST_DELAY)

            # ---- Page 4: Play-by-Play ----
            try:
                url = f"https://stats.ncaa.org/contests/{cid}/play_by_play"
                page.goto(url, wait_until="networkidle", timeout=30000)
                html = page.content()
                g_pbp = parse_play_by_play(html)
                status_parts.append(
                    f"play_by_play OK ({len(g_pbp['innings'])} innings)"
                )
            except Exception as e:
                status_parts.append(f"play_by_play FAILED ({e})")

            print(", ".join(status_parts))
            time.sleep(REQUEST_DELAY)

            # Store this game in the cache and immediately persist to disk.
            # Saving after each game means a mid-run crash won't lose already
            # scraped data — the next run will pick up where it left off.
            cache[cid] = {
                "contestId":         cid,
                "date":              game["date"],
                "opponent":          game["opponent"],
                "opponentTeamId":    game.get("opponentTeamId"),
                "isSEC":             game["isSEC"],
                "hitting":           g_hitting,
                "pitching":          g_pitching,
                "fielding":          g_fielding,
                "decisions":         g_decisions,
                "opponent_runs":     g_opp_runs,
                "hitting_splits":    list(g_hit_splits),   # tuple -> list for JSON
                "pitching_splits":   list(g_pitch_splits),
                "pitcher_list":      g_pitchers,
                "opponent_pitchers": g_opp_pitchers,
                "play_by_play":      g_pbp,
            }
            _save_cache(cache)

        # ==============================================================
        # Step 3b: PBP backfill for cached games without play_by_play
        #
        # These games were scraped before the PBP cache field existed.
        # We only fetch the one missing page and patch it into the
        # existing cache entry — no need to re-fetch the other 3 pages.
        # ==============================================================
        for i, game in enumerate(games_needing_pbp):
            cid = game["contestId"]
            label = (
                f"[{i + 1}/{len(games_needing_pbp)}] "
                f"PBP backfill for game {cid} "
                f"({game['date']} vs {game['opponent']})..."
            )
            print(label, end=" ")

            try:
                url = f"https://stats.ncaa.org/contests/{cid}/play_by_play"
                page.goto(url, wait_until="networkidle", timeout=30000)
                html = page.content()
                pbp = parse_play_by_play(html)
                cache[cid]["play_by_play"] = pbp
                _save_cache(cache)
                print(f"OK ({len(pbp['innings'])} innings)")
            except Exception as e:
                print(f"FAILED ({e})")

            time.sleep(REQUEST_DELAY)

        # ==============================================================
        # Step 3c: Opponent-pitchers backfill
        #
        # Cached games from before we started tracking the opposing team's
        # pitching table need a single page-1 fetch to grab it. We also
        # patch in opponentTeamId from the schedule lookup (the schedule
        # is already parsed into `games` above, keyed by contestId here).
        # ==============================================================
        games_by_cid = {g["contestId"]: g for g in games}
        for i, game in enumerate(games_needing_opp_pitchers):
            cid = game["contestId"]
            label = (
                f"[{i + 1}/{len(games_needing_opp_pitchers)}] "
                f"Opp pitchers backfill for game {cid} "
                f"({game['date']} vs {game['opponent']})..."
            )
            print(label, end=" ")

            try:
                url = f"https://stats.ncaa.org/contests/{cid}/individual_stats"
                page.goto(url, wait_until="networkidle", timeout=30000)
                html = page.content()
                opp_pitchers = parse_opponent_pitchers(html, TEAM_NAME)
                cache[cid]["opponent_pitchers"] = opp_pitchers
                # Also patch the opponent team id from the schedule entry
                schedule_entry = games_by_cid.get(cid, {})
                cache[cid]["opponentTeamId"] = schedule_entry.get("opponentTeamId")
                _save_cache(cache)
                print(f"OK ({len(opp_pitchers)} pitchers)")
            except Exception as e:
                print(f"FAILED ({e})")

            time.sleep(REQUEST_DELAY)

        # ==============================================================
        # Step 3d: Roster scraping for pitcher handedness
        #
        # For every unique opponent team we've ever played, visit their
        # /teams/{id}/roster page once and cache the pitcher name -> throws
        # map. On --full re-scrapes we refetch all rosters; otherwise we
        # only fetch rosters we don't yet have cached.
        # ==============================================================
        roster_cache = {} if args.full else load_roster_cache(ROSTER_CACHE_PATH)

        # Collect unique opponent team IDs across all games in the cache
        opp_team_ids = {}  # team_id -> opponent display name (most recent wins)
        for g in games:
            tid = cache.get(g["contestId"], {}).get("opponentTeamId")
            if tid:
                opp_team_ids[tid] = g["opponent"]

        rosters_to_fetch = [
            (tid, name) for tid, name in opp_team_ids.items()
            if tid not in roster_cache
        ]

        if not opp_team_ids:
            print("Roster scraping: no opponent team IDs available — skipping.\n")
        elif not rosters_to_fetch:
            print(
                f"Roster scraping: all {len(opp_team_ids)} opponent roster(s) "
                f"already cached.\n"
            )
        else:
            print(
                f"Roster scraping: {len(rosters_to_fetch)} new roster(s) to fetch "
                f"({len(opp_team_ids) - len(rosters_to_fetch)} already cached)."
            )
            for i, (tid, opp_name) in enumerate(rosters_to_fetch):
                label = (
                    f"[{i + 1}/{len(rosters_to_fetch)}] "
                    f"Fetching roster for {opp_name} (team {tid})..."
                )
                print(label, end=" ")
                try:
                    url = f"https://stats.ncaa.org/teams/{tid}/roster"
                    page.goto(url, wait_until="networkidle", timeout=30000)
                    html = page.content()
                    players = parse_roster(html)
                    roster_cache[tid] = {
                        "name":    opp_name,
                        "players": players,
                    }
                    save_roster_cache(ROSTER_CACHE_PATH, roster_cache)
                    print(f"OK ({len(players)} players)")
                except Exception as e:
                    print(f"FAILED ({e})")
                time.sleep(REQUEST_DELAY)
            print()

        # ==============================================================
        # Step 3e: Write public pitcher handedness file
        #
        # Emits public/data/pitcher-handedness-2026.json with one entry per
        # opponent team ID we've seen. Consumed downstream by parse_pbp.py.
        # ==============================================================
        if roster_cache:
            try:
                PITCHER_HAND_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
                with open(PITCHER_HAND_OUT_PATH, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "version": ROSTER_CACHE_VERSION,
                            "teams": roster_cache,
                        },
                        f,
                        indent=2,
                    )
                total_players = sum(
                    len(team.get("players", {})) for team in roster_cache.values()
                )
                print(
                    f"Wrote {PITCHER_HAND_OUT_PATH.name} "
                    f"({len(roster_cache)} teams, {total_players} players).\n"
                )
            except Exception as e:
                print(f"WARNING: failed to write {PITCHER_HAND_OUT_PATH.name}: {e}\n")

        browser.close()

    # ==============================================================
    # Step 4: Reconstruct parallel in-memory lists from the cache
    #
    # The aggregation functions below expect one entry per game in
    # schedule order. We rebuild those lists from the cache now,
    # regardless of whether this run scraped any new games.
    # ==============================================================
    all_hitting_games  = []
    all_pitching_games = []
    all_fielding_games = []
    all_decisions      = []
    all_opponent_runs  = []
    all_hitting_splits = []
    all_pitching_splits = []
    game_pitcher_lists = []

    for game in games:
        cid   = game["contestId"]
        entry = cache.get(cid, {})

        all_hitting_games.append( entry.get("hitting",  {"headers": [], "players": []}))
        all_pitching_games.append(entry.get("pitching", {"headers": [], "players": []}))
        all_fielding_games.append(entry.get("fielding", {"headers": [], "players": []}))
        all_decisions.append(    entry.get("decisions", {"win": None, "loss": None, "save": None}))
        all_opponent_runs.append(entry.get("opponent_runs", 0))
        game_pitcher_lists.append(entry.get("pitcher_list", []))

        hs = entry.get("hitting_splits", [[], []])
        all_hitting_splits.append(tuple(hs) if len(hs) == 2 else ([], []))

        ps = entry.get("pitching_splits", [[], []])
        all_pitching_splits.append(tuple(ps) if len(ps) == 2 else ([], []))

    # ==============================================================
    # Step 5: Build CG and SHO trackers
    # ==============================================================
    # A pitcher gets a CG if they were the ONLY pitcher listed for MSU in a game.
    # A pitcher gets a SHO if they got a CG AND the opponent scored 0 runs.
    cg_tracker = defaultdict(int)   # pitcher_name -> complete games
    sho_tracker = defaultdict(int)  # pitcher_name -> shutouts

    for game_idx, pitcher_list in enumerate(game_pitcher_lists):
        if len(pitcher_list) == 1:
            pitcher_name = pitcher_list[0]
            cg_tracker[pitcher_name] += 1
            # Check opponent runs for shutout
            if game_idx < len(all_opponent_runs) and all_opponent_runs[game_idx] == 0:
                sho_tracker[pitcher_name] += 1

    # We also need per-SEC-game CG/SHO trackers for the SEC subset
    cg_tracker_sec = defaultdict(int)
    sho_tracker_sec = defaultdict(int)

    for game_idx in sec_game_indices:
        if game_idx < len(game_pitcher_lists):
            pitcher_list = game_pitcher_lists[game_idx]
            if len(pitcher_list) == 1:
                pitcher_name = pitcher_list[0]
                cg_tracker_sec[pitcher_name] += 1
                if game_idx < len(all_opponent_runs) and all_opponent_runs[game_idx] == 0:
                    sho_tracker_sec[pitcher_name] += 1

    # And the same for non-conference games
    cg_tracker_nonsec = defaultdict(int)
    sho_tracker_nonsec = defaultdict(int)

    for game_idx in non_sec_game_indices:
        if game_idx < len(game_pitcher_lists):
            pitcher_list = game_pitcher_lists[game_idx]
            if len(pitcher_list) == 1:
                pitcher_name = pitcher_list[0]
                cg_tracker_nonsec[pitcher_name] += 1
                if game_idx < len(all_opponent_runs) and all_opponent_runs[game_idx] == 0:
                    sho_tracker_nonsec[pitcher_name] += 1

    # Also filter decisions for SEC games
    all_decisions_sec = [
        all_decisions[i] if i < len(all_decisions) else {"win": None, "loss": None, "save": None}
        for i in sorted(sec_game_indices)
    ]

    # And decisions for non-conference games
    all_decisions_nonsec = [
        all_decisions[i] if i < len(all_decisions) else {"win": None, "loss": None, "save": None}
        for i in sorted(non_sec_game_indices)
    ]

    # ==============================================================
    # Step 6: Aggregate individual stats (all games + SEC only)
    # ==============================================================
    print(f"\nAggregating individual stats...")

    hitting_agg_all = aggregate_individual_stats(all_hitting_games, "hitting")
    hitting_agg_sec = aggregate_individual_stats(all_hitting_games, "hitting", game_indices=sec_game_indices)
    hitting_agg_nonsec = aggregate_individual_stats(all_hitting_games, "hitting", game_indices=non_sec_game_indices)

    pitching_agg_all = aggregate_individual_stats(all_pitching_games, "pitching")
    pitching_agg_sec = aggregate_individual_stats(all_pitching_games, "pitching", game_indices=sec_game_indices)
    pitching_agg_nonsec = aggregate_individual_stats(all_pitching_games, "pitching", game_indices=non_sec_game_indices)

    fielding_agg_all = aggregate_individual_stats(all_fielding_games, "fielding")
    fielding_agg_sec = aggregate_individual_stats(all_fielding_games, "fielding", game_indices=sec_game_indices)
    fielding_agg_nonsec = aggregate_individual_stats(all_fielding_games, "fielding", game_indices=non_sec_game_indices)

    # ==============================================================
    # Step 7: Calculate derived stats
    # ==============================================================
    print("Calculating derived stats...")

    # Hitting
    hitting_players_all, hitting_summary_all = calculate_hitting_stats(hitting_agg_all, fielding_agg_all)
    hitting_players_sec, hitting_summary_sec = calculate_hitting_stats(hitting_agg_sec, fielding_agg_sec)
    hitting_players_nonsec, hitting_summary_nonsec = calculate_hitting_stats(hitting_agg_nonsec, fielding_agg_nonsec)

    # Pitching
    pitching_players_all, pitching_summary_all = calculate_pitching_stats(
        pitching_agg_all, all_decisions, cg_tracker, sho_tracker
    )
    pitching_players_sec, pitching_summary_sec = calculate_pitching_stats(
        pitching_agg_sec, all_decisions_sec, cg_tracker_sec, sho_tracker_sec
    )
    pitching_players_nonsec, pitching_summary_nonsec = calculate_pitching_stats(
        pitching_agg_nonsec, all_decisions_nonsec, cg_tracker_nonsec, sho_tracker_nonsec
    )

    # Fielding
    fielding_players_all, fielding_summary_all = calculate_fielding_stats(fielding_agg_all)
    fielding_players_sec, fielding_summary_sec = calculate_fielding_stats(fielding_agg_sec)
    fielding_players_nonsec, fielding_summary_nonsec = calculate_fielding_stats(fielding_agg_nonsec)

    # ==============================================================
    # Step 8: Aggregate situational splits (reusing existing logic)
    # ==============================================================
    print("Aggregating situational splits...")

    hitting_splits_all = aggregate_splits(all_hitting_splits)
    hitting_splits_sec = aggregate_splits(all_hitting_splits, game_indices=sec_game_indices)
    hitting_splits_nonsec = aggregate_splits(all_hitting_splits, game_indices=non_sec_game_indices)

    pitching_splits_all = aggregate_splits(all_pitching_splits)
    pitching_splits_sec = aggregate_splits(all_pitching_splits, game_indices=sec_game_indices)
    pitching_splits_nonsec = aggregate_splits(all_pitching_splits, game_indices=non_sec_game_indices)

    # ==============================================================
    # Step 9: Write all 5 JSON files
    # ==============================================================
    timestamp = datetime.now().isoformat(timespec="seconds")
    games_scraped = len(games)

    last_game_meta = {
        "date": last_game["date"],
        "opponent": last_game["opponent"],
    }

    # --- hitting-stats-2026.json ---
    hitting_stats_output = {
        "lastUpdated": timestamp,
        "gamesScraped": games_scraped,
        "lastGame": last_game_meta,
        "all": {
            "columns": HITTING_COLUMNS,
            "players": hitting_players_all,
            "summary": hitting_summary_all,
        },
        "sec": {
            "columns": HITTING_COLUMNS,
            "players": hitting_players_sec,
            "summary": hitting_summary_sec,
        },
        "nonsec": {
            "columns": HITTING_COLUMNS,
            "players": hitting_players_nonsec,
            "summary": hitting_summary_nonsec,
        },
    }

    hitting_stats_path = OUTPUT_DIR / "hitting-stats-2026.json"
    with open(hitting_stats_path, "w", encoding="utf-8") as f:
        json.dump(hitting_stats_output, f, indent=2)
    print(f"Wrote {hitting_stats_path}")

    # --- pitching-stats-2026.json ---
    pitching_stats_output = {
        "lastUpdated": timestamp,
        "gamesScraped": games_scraped,
        "lastGame": last_game_meta,
        "all": {
            "columns": PITCHING_COLUMNS,
            "players": pitching_players_all,
            "summary": pitching_summary_all,
        },
        "sec": {
            "columns": PITCHING_COLUMNS,
            "players": pitching_players_sec,
            "summary": pitching_summary_sec,
        },
        "nonsec": {
            "columns": PITCHING_COLUMNS,
            "players": pitching_players_nonsec,
            "summary": pitching_summary_nonsec,
        },
    }

    pitching_stats_path = OUTPUT_DIR / "pitching-stats-2026.json"
    with open(pitching_stats_path, "w", encoding="utf-8") as f:
        json.dump(pitching_stats_output, f, indent=2)
    print(f"Wrote {pitching_stats_path}")

    # --- fielding-stats-2026.json ---
    fielding_stats_output = {
        "lastUpdated": timestamp,
        "gamesScraped": games_scraped,
        "lastGame": last_game_meta,
        "all": {
            "columns": FIELDING_COLUMNS,
            "players": fielding_players_all,
            "summary": fielding_summary_all,
        },
        "sec": {
            "columns": FIELDING_COLUMNS,
            "players": fielding_players_sec,
            "summary": fielding_summary_sec,
        },
        "nonsec": {
            "columns": FIELDING_COLUMNS,
            "players": fielding_players_nonsec,
            "summary": fielding_summary_nonsec,
        },
    }

    fielding_stats_path = OUTPUT_DIR / "fielding-stats-2026.json"
    with open(fielding_stats_path, "w", encoding="utf-8") as f:
        json.dump(fielding_stats_output, f, indent=2)
    print(f"Wrote {fielding_stats_path}")

    # --- hitting-splits-2026.json (same format as scrape-splits.py) ---
    hitting_splits_output = {
        "lastUpdated": timestamp,
        "gamesScraped": len(all_hitting_splits),
        "lastGame": last_game_meta,
        "all": {
            "columns": hitting_splits_all["columns"],
            "players": hitting_splits_all["players"],
            "totals": hitting_splits_all["totals"],
        },
        "sec": {
            "columns": hitting_splits_sec["columns"],
            "players": hitting_splits_sec["players"],
            "totals": hitting_splits_sec["totals"],
        },
        "nonsec": {
            "columns": hitting_splits_nonsec["columns"],
            "players": hitting_splits_nonsec["players"],
            "totals": hitting_splits_nonsec["totals"],
        },
    }

    hitting_splits_path = OUTPUT_DIR / "hitting-splits-2026.json"
    with open(hitting_splits_path, "w", encoding="utf-8") as f:
        json.dump(hitting_splits_output, f, indent=2)
    print(f"Wrote {hitting_splits_path}")

    # --- pitching-splits-2026.json (same format as scrape-splits.py) ---
    pitching_splits_output = {
        "lastUpdated": timestamp,
        "gamesScraped": len(all_pitching_splits),
        "lastGame": last_game_meta,
        "all": {
            "columns": pitching_splits_all["columns"],
            "players": pitching_splits_all["players"],
            "totals": pitching_splits_all["totals"],
        },
        "sec": {
            "columns": pitching_splits_sec["columns"],
            "players": pitching_splits_sec["players"],
            "totals": pitching_splits_sec["totals"],
        },
        "nonsec": {
            "columns": pitching_splits_nonsec["columns"],
            "players": pitching_splits_nonsec["players"],
            "totals": pitching_splits_nonsec["totals"],
        },
    }

    pitching_splits_path = OUTPUT_DIR / "pitching-splits-2026.json"
    with open(pitching_splits_path, "w", encoding="utf-8") as f:
        json.dump(pitching_splits_output, f, indent=2)
    print(f"Wrote {pitching_splits_path}")

    # --- schedule-2026.json ---
    # Build schedule entries from the full schedule + cached box-score data.
    # For completed games, derive the result (W/L) and score from cache.
    schedule_games = []
    for entry in full_schedule:
        game_out = {
            "date": entry["date"],
            "opponent": entry["opponent"],
            "location": "Away" if entry["isAway"] else "Home",
            "isSEC": entry["isSEC"],
            "result": None,
            "attendance": None,
        }

        cid = entry.get("contestId")
        if cid and cid in cache:
            cached = cache[cid]
            opp_runs = cached.get("opponent_runs", 0)

            # Sum MSU runs from hitting box-score data.
            # Players are stored as dicts keyed by column name.
            hit_data = cached.get("hitting", {})
            hit_players = hit_data.get("players", [])
            msu_runs = 0
            for p in hit_players:
                try:
                    val = p.get("R", 0) if isinstance(p, dict) else p[2]
                    msu_runs += int(val)
                except (ValueError, IndexError, KeyError, TypeError):
                    pass

            # Determine W/L
            if msu_runs > opp_runs:
                game_out["result"] = f"W {msu_runs}-{opp_runs}"
            elif msu_runs < opp_runs:
                game_out["result"] = f"L {msu_runs}-{opp_runs}"
            else:
                game_out["result"] = f"T {msu_runs}-{opp_runs}"

        schedule_games.append(game_out)

    schedule_output = {
        "lastUpdated": timestamp,
        "gamesScraped": games_scraped,
        "lastGame": last_game_meta,
        "games": schedule_games,
    }

    schedule_path = OUTPUT_DIR / "schedule-2026.json"
    with open(schedule_path, "w", encoding="utf-8") as f:
        json.dump(schedule_output, f, indent=2)
    print(f"Wrote {schedule_path}")

    # ==============================================================
    # Summary
    # ==============================================================
    new_count = len(games_to_scrape)
    pbp_backfill_count = len(games_needing_pbp)
    # New games fetch 4 pages each; PBP backfill fetches 1 page each
    page_loads = new_count * 4 + pbp_backfill_count
    print(f"\nDone! {games_scraped} total games processed.")
    print(f"  Newly scraped:  {new_count} game(s)")
    print(f"  PBP backfill:   {pbp_backfill_count} game(s)")
    print(f"  Total page loads: {page_loads}")
    print(f"  From cache:     {cached_count} game(s)")
    print(f"  Hitters:  {len(hitting_players_all)} players")
    print(f"  Pitchers: {len(pitching_players_all)} players")
    print(f"  Fielders: {len(fielding_players_all)} players")
    print(f"  Splits:   {len(hitting_splits_all['players'])} hitters, "
          f"{len(pitching_splits_all['players'])} pitchers")

    # PBP coverage across the full cache
    pbp_covered = sum(
        1 for g in games
        if cache.get(g["contestId"], {}).get("play_by_play", {}).get("innings")
    )
    print(f"  PBP cached:     {pbp_covered} / {len(games)} games")

    # ==============================================================
    # SEC Standings (secsports.com) — runs after all NCAA scraping is done
    # ==============================================================
    print("\n" + "=" * 60)
    print("Scraping SEC standings...")
    print("=" * 60)
    try:
        scrape_sec_standings(headless=args.headless)
    except Exception as e:
        print(f"WARNING: SEC standings scrape failed: {e}")
        print("  The rest of the data was written successfully.")


if __name__ == "__main__":
    main()
