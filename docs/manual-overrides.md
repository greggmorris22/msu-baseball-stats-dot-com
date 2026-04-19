# Manual overrides

NCAA data has gaps and quirks. We patch them with a small set of hand-maintained
overrides. **All overrides listed here are persistent** — a full re-scrape
(`rm data/scrape-cache.json` followed by `scripts/scrape-stats.py`) will *not*
wipe them, because they live in files/code that are committed to git.

This document is just an index — each individual override source has inline
comments explaining its format and how to add entries.

---

## 1. Pitcher handedness overrides

**File:** [`data/pitcher-hand-overrides.json`](../data/pitcher-hand-overrides.json)

**Why it exists:** NCAA rosters sometimes have a blank "Throws" field, or the
pitcher's name in the play-by-play is truncated/mis-spelled and doesn't match
any roster entry. Without a hand, the Batting Splits Tool cannot bucket a PA as
vs-LHP or vs-RHP.

**Used by:** `scripts/parse_pbp.py` → `_lookup_hand_side()` (as a **fallback**
after the team roster cache lookup). This ordering matters: the roster is the
primary source of truth, and the override file is only consulted when the
roster has no answer. That avoids ambiguous last-name collisions (e.g. Brady
Richardson on Troy is R; Corey Richardson on Jackson State is L — the roster
disambiguates them correctly, and we must not let an override force both to the
same hand).

**Format:** `{ "Full Name or Last Name,": "R" | "L" | "S" }` — keys match what
appears in the PBP text. Values are single characters. See inline comments in
the file itself for guidance on finding a pitcher's hand.

**Current entries (as of 04/18/2026):**

| Pitcher | Team | Hand | Notes |
|---|---|---|---|
| Doug Marose | Hofstra | R | NCAA roster blank |
| Jonah Richardson | Troy | R | NCAA roster blank |
| Jonah Richar | Troy | R | Truncated PBP form of Jonah Richardson |
| Brady Blum | Delaware | R | NCAA roster blank |
| Elias Conway | Delaware | L | NCAA roster blank |
| Jake Pollaro | Delaware | L | NCAA roster blank |
| Kevin Landry | Southern Miss | R | NCAA roster blank |
| Kevin Landry Farr | Southern Miss | R | Alternate name form |
| Corey Richardson | Jackson St. | L | NCAA roster blank (redundant now — Jackson St.'s roster cache has L, but kept as a safety net) |

To find handedness for a new entry, look the pitcher up on their team's
official roster page and note their throwing hand.

---

## 2. Game locations (city, state)

**Source:** [`scripts/scrape-stats.py`](../scripts/scrape-stats.py) — the
`GAME_LOCATIONS` dict near the top of the file.

**Why it exists:** The NCAA schedule page only says `Home` / `Away`, which
doesn't tell us *where* away games are played, and doesn't distinguish true
road trips from neutral-site tournaments (Arlington, Biloxi, Pearl). We display
the actual city/state on the Schedule page, which requires a hand-built map.

**Used by:** `scripts/scrape-stats.py` → the schedule-building loop around the
`_lookup_game_location()` call. The key is the game date (`MM/DD/YYYY`) with an
optional doubleheader suffix `(1)`/`(2)`. The lookup normalizes the raw NCAA
schedule date by stripping any trailing `" HH:MM XM"` or `" TBA"` so the dict
stays stable even after NCAA drops the time once a game is played.

**Format:**

```python
GAME_LOCATIONS: dict[str, str] = {
    "02/13/2026":    "Starkville, MS",    # vs Hofstra
    "02/14/2026(1)": "Starkville, MS",    # DH game 1
    "02/14/2026(2)": "Starkville, MS",    # DH game 2
    ...
}
```

**Maintenance:** Update this dict at the start of each season once the schedule
is finalized. If a new mid-season game is added (rain make-up, etc.), add its
entry here.

---

## 3. Things that are *not* overrides (but might look like them)

- `data/roster-cache.json` — **rebuilt on every scrape**, so any manual edits
  are wiped. Don't hand-edit this file. If a pitcher's handedness is wrong or
  missing in the NCAA roster, add them to
  `data/pitcher-hand-overrides.json` instead.
- `data/scrape-cache.json` — transient per-game HTML cache. Safe to delete for
  a full re-scrape. Contains no manual overrides.
- `public/data/*.json` — generated output files. Regenerated on every scrape;
  never hand-edit.

---

## 4. Full re-scrape checklist

When you need to burn down and rebuild from scratch (e.g. after adding new
fields to the scraper):

1. `rm data/scrape-cache.json`
2. Run the headed scraper: `.venv/Scripts/python.exe scripts/scrape-stats.py`
   (NCAA blocks headless Playwright — always use the headed default.)
3. Run the PBP walker: `.venv/Scripts/python.exe scripts/parse_pbp.py`
4. The two override sources above are **untouched** by this process — you do
   not need to re-enter them.
