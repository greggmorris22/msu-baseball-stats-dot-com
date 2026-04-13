"""
stat_helpers.py — Derived fantasy baseball stat calculations.

All functions here are pure (no I/O, no side effects) and operate either
on scalar values or on pandas DataFrames using vectorized operations.
They are easy to unit-test in isolation.

Stat definitions for this league
---------------------------------
TB  = Total Bases     = 1B + (2 × 2B) + (3 × 3B) + (4 × HR)
NSB = Net Stolen Bases = SB - CS
NSV = Net Saves       = SV - BS
QA3 = Quality App. 3  = SP starts with 5+ IP AND ERA <= 4.50
      (estimated from season totals using a rate model — see calc_qa3)
"""

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Scalar calculation functions
# ---------------------------------------------------------------------------


def calc_tb(h: float, double: float, triple: float, hr: float) -> float:
    """
    Calculate Total Bases from counting stats.

    The standard formula is:
      TB = 1B + (2 × 2B) + (3 × 3B) + (4 × HR)

    FanGraphs projections give us total H (which includes 2B, 3B, HR)
    rather than singles directly. So we first derive singles:
      1B = H - 2B - 3B - HR

    Then: TB = (H - 2B - 3B - HR) + (2 × 2B) + (3 × 3B) + (4 × HR)

    Parameters
    ----------
    h      : total hits (includes 1B, 2B, 3B, HR)
    double : doubles (2B)
    triple : triples (3B)
    hr     : home runs

    Returns
    -------
    float
    """
    singles = h - double - triple - hr
    return singles + (2.0 * double) + (3.0 * triple) + (4.0 * hr)


def calc_nsb(sb: float, cs: float) -> float:
    """
    Calculate Net Stolen Bases.

    NSB = SB - CS

    Negatives are intentional — bad base-stealers (high CS) should be
    penalized in the Z-score calculation.

    Parameters
    ----------
    sb : stolen bases
    cs : caught stealing

    Returns
    -------
    float
    """
    return sb - cs


def calc_nsv(sv: float, bs: float) -> float:
    """
    Calculate Net Saves.

    NSV = SV - BS

    Parameters
    ----------
    sv : saves
    bs : blown saves

    Returns
    -------
    float
    """
    return sv - bs


def calc_qa3(gs: float, ip: float, era: float) -> float:
    """
    Estimate Quality Appearance 3 (QA3) count from season totals.

    QA3 is defined as a start in which the pitcher records 5+ IP with
    an ERA <= 4.50 for that outing. Since Steamer projections give us
    only season-level totals (not per-start distributions), we estimate
    QA3 using two rate factors:

    IP factor  — how often does this pitcher reach 5 innings per start?
      ip_factor = clamp((avg_ip_per_start - 3.5) / 3.0, 0.0, 1.0)
      At avg_ip = 3.5: factor = 0.0 (never reaches 5 IP)
      At avg_ip = 6.5: factor = 1.0 (nearly always reaches 5 IP)

    ERA factor — how often does this pitcher allow <= 4.50 ERA in a start?
      era_factor = clamp((7.0 - ERA) / 4.0, 0.0, 1.0)
      At ERA = 7.0: factor = 0.0 (almost no QA starts)
      At ERA = 3.0: factor = 1.0 (nearly all starts qualify on ERA)

    QA3 = GS × ip_factor × era_factor

    Parameters
    ----------
    gs  : games started (season total)
    ip  : innings pitched (season total)
    era : projected ERA

    Returns
    -------
    float
        Estimated number of quality appearances. Returns 0.0 if gs == 0.
    """
    if gs <= 0:
        return 0.0

    avg_ip = ip / gs
    ip_factor = float(np.clip((avg_ip - 3.5) / 3.0, 0.0, 1.0))
    era_factor = float(np.clip((7.0 - era) / 4.0, 0.0, 1.0))
    return gs * ip_factor * era_factor


# ---------------------------------------------------------------------------
# DataFrame-level helpers (vectorized)
# ---------------------------------------------------------------------------


def add_derived_batting_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add TB and NSB columns to a batter projections DataFrame.

    Requires columns: h, double, triple, hr, sb, cs
    Adds columns:     tb, nsb

    Parameters
    ----------
    df : pd.DataFrame
        Raw batter projections. Modified columns are added; originals preserved.

    Returns
    -------
    pd.DataFrame
        A new DataFrame with the added columns.
    """
    df = df.copy()
    # TB = 1B + 2*2B + 3*3B + 4*HR  (derive singles from total H first)
    singles = df["h"] - df["double"] - df["triple"] - df["hr"]
    df["tb"] = singles + (2.0 * df["double"]) + (3.0 * df["triple"]) + (4.0 * df["hr"])
    df["nsb"] = df["sb"] - df["cs"]
    return df


def add_derived_pitching_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add QA3 and NSV columns to a pitcher projections DataFrame.

    Requires columns: gs, ip, era, sv, bs
    Adds columns:     qa3, nsv

    Parameters
    ----------
    df : pd.DataFrame
        Raw pitcher projections. Modified columns are added; originals preserved.

    Returns
    -------
    pd.DataFrame
        A new DataFrame with the added columns.
    """
    df = df.copy()

    # NSV = SV - BS
    df["nsv"] = df["sv"] - df["bs"]

    # QA3 estimated via vectorized operations
    gs = df["gs"].fillna(0.0)
    ip = df["ip"].fillna(0.0)
    era = df["era"].fillna(9.99)  # pessimistic default for missing ERA

    avg_ip = ip.where(gs > 0, 0.0) / gs.where(gs > 0, 1.0)
    ip_factor = ((avg_ip - 3.5) / 3.0).clip(0.0, 1.0)
    era_factor = ((7.0 - era) / 4.0).clip(0.0, 1.0)
    df["qa3"] = (gs * ip_factor * era_factor).round(1)

    return df
