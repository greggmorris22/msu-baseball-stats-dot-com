"""
parse_li_table.py — One-off scraper/parser that downloads Tango's Leverage
Index table from insidethebook.com/li.shtml and converts it into a compact
JSON lookup used by parse_pbp.py.

The source page has 18 half-inning sections (Top 1, Bottom 1, ..., Top 9,
Bottom 9). Within each section, a single HTML table has:
  - A header row: "1 2 3 | Outs | -4 | -3 | -2 | -1 | 00 | +1 | +2 | +3 | +4"
  - 24 data rows: 8 base states x 3 out counts
  - Score differentials are from the batting team's perspective.

Blank cells on the page mean "low leverage" (Tango shaded high cells and
left low ones blank to save visual space). We assign blanks an LI of 0.5
so they bucket into the Low category without skewing the sort.

Output: data/leverage-index.json, structured as a nested dict:
    {
      "1_top":    { "___": { "0": { "-4": 0.4, ... "+4": 0.5 } } }  # 1B 2B 3B
      "1_bottom": { ... },
      ...
      "9_bottom": { ... },
    }

Base state keys use three chars (`_` = empty, `1`/`2`/`3` = runner on base):
    "___"  bases empty
    "1__"  runner on 1st
    "_2_"  runner on 2nd
    "__3"  runner on 3rd
    "12_"  runners on 1st and 2nd
    "1_3"  runners on 1st and 3rd
    "_23"  runners on 2nd and 3rd
    "123"  bases loaded

Usage:
    .venv/Scripts/python.exe scripts/parse_li_table.py
"""

import json
import re
import urllib.request
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR   = SCRIPT_DIR.parent
SRC_URL    = "http://www.insidethebook.com/li.shtml"
OUTPUT_PATH = ROOT_DIR / "data" / "leverage-index.json"

# Extra-inning fallback: insidethebook only publishes through inning 9.
# For innings 10+, common sabermetric practice is to reuse the bottom-9
# values because the game is (by definition) tied entering extras and
# every half-inning has walkoff potential. We'll just clone inning 9 in
# post-processing below.


# ===========================================================================
# HTML parsing
# ===========================================================================

# Each section header looks like:
#   <DIV class='g'>Top of Inning 1</DIV>
# or
#   <DIV class='gray'>Bottom of Inning 3</DIV>
SECTION_RE = re.compile(
    r"(Top|Bottom) of Inning (\d+)</DIV>(.*?)(?=(?:Top|Bottom) of Inning|\Z)",
    re.DOTALL,
)

# A data row looks like:
#   <TR><TD>1 _ _<TD>0<TD>0.7<TD><DIV class='xg'>0.9</DIV><TD>...
# We split a section by <TR>, then pull <TD> cells out of each.
TR_SPLIT_RE = re.compile(r"<TR[^>]*>", re.IGNORECASE)
TD_SPLIT_RE = re.compile(r"<TD[^>]*>", re.IGNORECASE)

# Base state token normaliser:
#   "_ _ _"  ->  "___"
#   "1 _ _"  ->  "1__"
#   "1 2 3"  ->  "123"
BASE_STATE_RE = re.compile(r"^[_123]\s+[_123]\s+[_123]$")

SCORE_DIFF_COLUMNS = ["-4", "-3", "-2", "-1", "0", "+1", "+2", "+3", "+4"]

# Default LI to assign when the source page shows a blank cell. These cells
# are explicitly noted as "low leverage" on insidethebook.com, so any value
# below our Low cutoff (0.85) works. 0.5 is a reasonable middle-of-low
# default that matches the general shape of the published table.
BLANK_LI = 0.5


def strip_tags(text):
    """Remove HTML tags and collapse whitespace."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ")
    return re.sub(r"\s+", " ", text).strip()


def parse_section(body):
    """
    Parse one half-inning's worth of HTML into a dict
        { base_state: { outs: { score_diff: li_value } } }

    Args:
        body: Raw HTML between two section headers.

    Returns:
        Nested dict or empty dict if parsing fails.
    """
    rows = TR_SPLIT_RE.split(body)
    out = {}
    for row in rows:
        # Grab every <TD> cell content
        cells = [strip_tags(c) for c in TD_SPLIT_RE.split(row)[1:]]
        if len(cells) < 11:
            continue
        base_state_raw = cells[0]
        outs_raw       = cells[1]

        if not BASE_STATE_RE.match(base_state_raw):
            continue
        if outs_raw not in ("0", "1", "2"):
            continue

        base_key = base_state_raw.replace(" ", "")
        outs_key = outs_raw

        # Next 9 cells are the score-diff columns (-4, -3, -2, -1, 00, +1..+4).
        # Missing / blank -> BLANK_LI.
        li_cells = cells[2:11]
        row_out = {}
        for col_label, raw in zip(SCORE_DIFF_COLUMNS, li_cells):
            raw = raw.strip()
            if not raw:
                row_out[col_label] = BLANK_LI
                continue
            try:
                row_out[col_label] = float(raw)
            except ValueError:
                row_out[col_label] = BLANK_LI

        out.setdefault(base_key, {}).setdefault(outs_key, {}).update(row_out)

    return out


def parse_html(html):
    """
    Parse the full HTML page into a dict keyed by "{inning}_{half}".
    """
    out = {}
    for m in SECTION_RE.finditer(html):
        half    = m.group(1).lower()   # "top" or "bottom"
        inning  = int(m.group(2))
        body    = m.group(3)
        section = parse_section(body)
        if section:
            out[f"{inning}_{half}"] = section
    return out


# ===========================================================================
# Main
# ===========================================================================

def main():
    print(f"Fetching {SRC_URL}...")
    req = urllib.request.Request(SRC_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        html = r.read().decode("latin-1")
    print(f"  got {len(html):,} bytes")

    table = parse_html(html)
    print(f"Parsed {len(table)} half-inning sections")

    # Validate every expected section is present
    missing = []
    for inning in range(1, 10):
        for half in ("top", "bottom"):
            key = f"{inning}_{half}"
            if key not in table:
                missing.append(key)
    if missing:
        print(f"WARNING: missing sections: {missing}")

    # Count total cells
    total_cells = 0
    for section in table.values():
        for base_state, by_outs in section.items():
            for outs, by_diff in by_outs.items():
                total_cells += len(by_diff)
    print(f"Total LI cells: {total_cells} (expected 9 inn x 2 half x 8 base x 3 out x 9 diff = 3888)")

    # Clone inning 9 for extra innings (10-15), capping at 15 for safety
    for extra in range(10, 16):
        table[f"{extra}_top"]    = table.get("9_top", {})
        table[f"{extra}_bottom"] = table.get("9_bottom", {})

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(table, f, indent=2)
    print(f"Wrote {OUTPUT_PATH} ({OUTPUT_PATH.stat().st_size / 1024:.1f} KB)")

    # Spot-check a known value: Top of 1, 0 outs, bases empty, tied = 0.9
    spot = table.get("1_top", {}).get("___", {}).get("0", {}).get("0")
    print(f"\nSpot check: Top 1, 0 outs, bases empty, tied = {spot} (expected 0.9)")


if __name__ == "__main__":
    main()
