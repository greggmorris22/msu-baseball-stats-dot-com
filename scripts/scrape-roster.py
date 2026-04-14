"""
scrape-roster.py — One-time scrape of the MSU baseball roster from stats.ncaa.org.

Navigates to the team roster page, parses the roster table, and writes the
result to public/data/roster-2026.json for display on the Roster page.

Usage (from project root):
    .venv/Scripts/python.exe scripts/scrape-roster.py

    # Headless mode (CI / no visible window):
    .venv/Scripts/python.exe scripts/scrape-roster.py --headless

Requirements (already in scripts/requirements.txt):
    playwright, beautifulsoup4
    playwright install chromium  (one-time setup)
"""

import argparse
import json
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


# ===========================================================================
# Configuration
# ===========================================================================

ROSTER_URL = "https://stats.ncaa.org/teams/614666/roster"

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_PATH = SCRIPT_DIR.parent / "public" / "data" / "roster-2026.json"

# Stealth script for headless mode — masks common bot-detection signals
STEALTH_SCRIPT = """
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    window.chrome = { runtime: {} };
"""


# ===========================================================================
# Scraping
# ===========================================================================

def parse_roster_table(html: str) -> tuple[list[str], list[list[str]]]:
    """
    Parse the roster HTML page and extract column headers and player rows.

    The NCAA roster page has a single table with a <thead> row of column
    headers and <tbody> rows for each player. We read the headers dynamically
    so the script won't break if the NCAA adjusts column order.

    Args:
        html: Full HTML string of the roster page.

    Returns:
        A tuple of (columns, players) where:
          - columns: list of header strings, e.g. ["#", "Name", "Pos", ...]
          - players: list of rows, each row a list of cell strings
    """
    soup = BeautifulSoup(html, "html.parser")

    # The html argument is either the roster table's outerHTML (from
    # page.evaluate) or a full page HTML string — find the table either way.
    # Prefer a table that has a "Name" header cell.
    table = soup.find("table")  # fast path: direct table HTML from evaluate()
    if table is None or "Name" not in [th.get_text(strip=True) for th in table.find_all("th")]:
        table = None
        for t in soup.find_all("table"):
            headers = [th.get_text(strip=True) for th in t.find_all("th")]
            if "Name" in headers:
                table = t
                break

    if table is None:
        raise RuntimeError(
            "Could not find a roster table on the page. "
            "The page structure may have changed, or the page did not load fully."
        )

    # --- Headers ---
    columns = [th.get_text(strip=True) for th in table.find_all("th")]

    # --- Player rows ---
    players = []
    tbody = table.find("tbody")
    rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]  # skip header row

    for row in rows:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        row_data = [cell.get_text(strip=True) for cell in cells]
        # Skip blank rows and totals rows (NCAA sometimes adds a separator)
        if all(val == "" for val in row_data):
            continue
        players.append(row_data)

    return columns, players


# ===========================================================================
# Main
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Scrape MSU baseball roster from stats.ncaa.org.")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run in headless mode (for CI). Uses stealth evasion to bypass bot detection.",
    )
    args = parser.parse_args()

    print(f"Scraping roster from: {ROSTER_URL}")
    print(f"Mode: {'headless (stealth)' if args.headless else 'headed (visible Chrome)'}")
    print(f"Output: {OUTPUT_PATH}\n")

    with sync_playwright() as pw:
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

        print("Loading roster page...")
        page.goto(ROSTER_URL, wait_until="networkidle", timeout=30000)

        # The NCAA roster table is a DataTables widget. The data may be:
        #   (a) loaded via AJAX after the page renders, or
        #   (b) embedded in the page as a JavaScript array (common with DataTables)
        # Strategy: intercept network responses to capture the AJAX payload if
        # one fires, then fall back to reading from the DataTables JS API.

        # The NCAA roster page is server-rendered: the full table rows are in
        # the initial HTML response BEFORE JavaScript runs. DataTables then
        # re-processes the DOM, clearing the tbody and re-rendering paged rows.
        # We capture the raw server HTML via the response handler so we get the
        # pre-JavaScript version with all rows intact.
        raw_html = {}

        def handle_response(response):
            """Capture the initial HTML page response before JS modifies it."""
            url = response.url
            ct = response.headers.get("content-type", "")
            if "html" in ct and url.rstrip("/").endswith(ROSTER_URL.rstrip("/")):
                try:
                    raw_html["content"] = response.text()
                    print(f"  Captured raw HTML response ({len(raw_html['content'])} bytes)")
                except Exception as e:
                    print(f"  WARNING: Could not read response body: {e}")

        page.on("response", handle_response)

        print("Loading roster page...")
        page.goto(ROSTER_URL, wait_until="networkidle", timeout=30000)

        browser.close()

    # Use the captured server HTML (pre-JavaScript) if available;
    # fall back to the current DOM HTML (post-JavaScript) otherwise.
    html = raw_html.get("content", "")
    if not html:
        print("WARNING: Could not capture raw server HTML. Falling back to JS-modified DOM.")

    print("Parsing roster table...")
    columns, players = parse_roster_table(html)
    print(f"  Columns ({len(columns)}): {columns}")
    print(f"  Player rows: {len(players)}")

    # Drop GP and GS columns (games played / games started) — not needed
    # on the roster page. Also normalize Bats/Throws capitalization.
    drop = {'GP', 'GS'}
    keep_idx = [i for i, c in enumerate(columns) if c not in drop]
    columns = [columns[i] for i in keep_idx]
    players = [[row[i] for i in keep_idx] for row in players]

    cap_map = {'RIGHT': 'Right', 'LEFT': 'Left', 'BOTH': 'Both'}
    bats_idx = columns.index('Bats') if 'Bats' in columns else None
    throws_idx = columns.index('Throws') if 'Throws' in columns else None
    for row in players:
        if bats_idx is not None:
            row[bats_idx] = cap_map.get(row[bats_idx], row[bats_idx])
        if throws_idx is not None:
            row[throws_idx] = cap_map.get(row[throws_idx], row[throws_idx])

    print(f"  Final columns: {columns}")

    output = {
        "scraped": datetime.now().isoformat(timespec="seconds"),
        "source": ROSTER_URL,
        "columns": columns,
        "players": players,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nSaved to: {OUTPUT_PATH}")
    print("Done.")


if __name__ == "__main__":
    main()
