"""
scrape-sec-standings.py — Scrape the SEC baseball standings from secsports.com.

The page is JavaScript-rendered (Sidearm Sports platform), so Playwright is used
to load the page fully before extracting the table.

Outputs:
    public/data/sec-standings-2026.json

    JSON format:
    {
      "lastUpdated": "2026-04-14T15:30:00",
      "source": "https://www.secsports.com/standings/baseball",
      "columns": ["Team", "W", "L", "Pct", "GB", "Home", "Away", ...],
      "teams": [
        ["Mississippi St.", "7", "7", ".500", "3.5", "4-2", "3-5", ...],
        ...
      ]
    }

Usage (from project root):
    .venv/Scripts/python.exe scripts/scrape-sec-standings.py

    # Headless mode:
    .venv/Scripts/python.exe scripts/scrape-sec-standings.py --headless

This script can also be called as a module from scrape-stats.py:
    from scrape_sec_standings import scrape_sec_standings
    scrape_sec_standings(headless=False)

Requirements (already in .venv):
    playwright, beautifulsoup4
    playwright install chromium  (one-time setup)
"""

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page


# ===========================================================================
# Configuration
# ===========================================================================

STANDINGS_URL = "https://www.secsports.com/standings/baseball"

SCRIPT_DIR   = Path(__file__).resolve().parent
OUTPUT_DIR   = SCRIPT_DIR.parent / "public" / "data"
OUTPUT_PATH  = OUTPUT_DIR / "sec-standings-2026.json"

# Stealth script — masks common Playwright/Chromium bot-detection signals.
# The same script is used in scrape-roster.py and scrape-stats.py.
STEALTH_SCRIPT = """
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    window.chrome = { runtime: {} };
"""


# ===========================================================================
# Parsing
# ===========================================================================

def parse_standings_table(html: str) -> tuple[list[str], list[list[str]]]:
    """
    Parse the rendered standings page HTML and extract column headers and rows.

    secsports.com uses the Sidearm Sports platform and renders the standings
    into a <table class="ui-table sport-standings-table__table"> after JavaScript
    runs. The table headers are lowercase abbreviations; we map them to
    human-readable labels.

    Args:
        html: Full HTML string of the fully-rendered standings page.

    Returns:
        A tuple of (columns, teams) where:
          - columns: list of human-readable header strings
          - teams:   list of rows, each a list of cell strings

    Raises:
        RuntimeError: If no recognizable standings table can be found.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Primary selector: Sidearm Sports standings table class
    target_table = soup.find("table", class_="ui-table")

    # Fallback: any table whose headers include "conf" and "overall"
    if target_table is None:
        for table in soup.find_all("table"):
            raw_headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if "conf" in raw_headers and "overall" in raw_headers:
                target_table = table
                break

    if target_table is None:
        raise RuntimeError(
            "Could not find a standings table on the page. "
            "The page may not have loaded fully, or the site structure has changed."
        )

    # --- Column headers ---
    # The raw headers from secsports.com are lowercase abbreviations.
    # Map them to human-readable display labels.
    HEADER_MAP = {
        "":        "Team",
        "conf":    "Conf W-L",
        "cpct":    "Conf Pct",
        "overall": "Overall W-L",
        "opct":    "Overall Pct",
        "home":    "Home",
        "road":    "Road",
        "neutral": "Neutral",
        "strk":    "Strk",
    }
    raw_headers = [th.get_text(strip=True).lower() for th in target_table.find_all("th")]
    columns = [HEADER_MAP.get(h, h.title()) for h in raw_headers]

    # --- Team rows ---
    teams = []
    tbody = target_table.find("tbody")
    rows = tbody.find_all("tr") if tbody else target_table.find_all("tr")[1:]

    for row in rows:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        row_data = [cell.get_text(strip=True) for cell in cells]
        # Skip entirely blank rows or divider rows
        if all(val == "" for val in row_data):
            continue
        teams.append(row_data)

    return columns, teams


# ===========================================================================
# Scraping
# ===========================================================================

def scrape_sec_standings(headless: bool = False) -> dict:
    """
    Launch Playwright, load the SEC standings page, parse the table, and write
    the result to OUTPUT_PATH.

    Args:
        headless: If True, run Chromium without a visible window (CI mode).

    Returns:
        The parsed standings dict (same structure as what is written to JSON).

    Raises:
        RuntimeError: If the standings table cannot be found after the page loads.
    """
    print(f"Scraping SEC standings from: {STANDINGS_URL}")
    print(f"Mode: {'headless' if headless else 'headed (visible Chrome)'}")
    print(f"Output: {OUTPUT_PATH}\n")

    with sync_playwright() as pw:
        if headless:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
                locale="en-US",
            )
            context.add_init_script(STEALTH_SCRIPT)
        else:
            browser = pw.chromium.launch(headless=False, channel="chrome")
            context = browser.new_context(viewport={"width": 1280, "height": 900})

        page = context.new_page()

        print("Loading standings page (waiting for network idle)...")
        page.goto(STANDINGS_URL, wait_until="networkidle", timeout=45000)

        # Give JS frameworks extra time to render the table after network idle.
        # Sidearm Sports often fires a second render pass after the initial load.
        print("Waiting for standings table to appear in DOM...")
        try:
            # Wait for the Sidearm Sports standings table specifically
            page.wait_for_selector("table.ui-table", timeout=15000)
        except Exception:
            # Fall back to any table if the specific class isn't found
            try:
                page.wait_for_selector("table", timeout=5000)
                print("  (fell back to generic <table> selector)")
            except Exception:
                print("WARNING: Timed out waiting for <table>. Attempting to parse anyway.")

        # Extra safety pause — Sidearm pages sometimes re-render after network idle
        time.sleep(2)

        html = page.content()
        browser.close()

    print("Parsing standings table...")
    columns, teams = parse_standings_table(html)
    print(f"  Columns ({len(columns)}): {columns}")
    print(f"  Team rows: {len(teams)}")
    for t in teams:
        print(f"    {t}")

    output = {
        "lastUpdated": datetime.now().isoformat(timespec="seconds"),
        "source": STANDINGS_URL,
        "columns": columns,
        "teams": teams,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(f"\nWrote {OUTPUT_PATH}")

    return output


# ===========================================================================
# CLI entry point
# ===========================================================================

def main():
    """Parse CLI arguments and run the scrape."""
    parser = argparse.ArgumentParser(
        description="Scrape SEC baseball standings from secsports.com."
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run in headless mode (no visible browser window). For CI use.",
    )
    args = parser.parse_args()

    scrape_sec_standings(headless=args.headless)


if __name__ == "__main__":
    main()
