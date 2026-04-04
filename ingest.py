import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from nba_api.stats.endpoints import leaguedashplayerstats

load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL"))

# ── helpers ──────────────────────────────────────────────────────────────────

def upsert(df: pd.DataFrame, table: str, conflict_column: str):
    """
    Clears the table and reloads fresh data on each run.
    Simple and safe for a daily pipeline.
    """
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table}"))
    df.to_sql(table, engine, if_exists="append", index=False)
    print(f"✓ {table}: {len(df)} rows written")

# ── fetch advanced stats ──────────────────────────────────────────────────────

def fetch_advanced_stats():
    url = "https://www.basketball-reference.com/leagues/NBA_2026_advanced.html"
    bbref = pd.read_html(url)[0]

    traded_players = bbref[bbref.Team.str.contains(r"\dTM", na=False)]["Player"].unique()
    bbref_clean = bbref[
        (bbref.Team.str.contains(r"\dTM", na=False)) |
        (~bbref["Player"].isin(traded_players))
    ].reset_index(drop=True)

    bbref_clean = (
        bbref_clean[["Player", "BPM", "VORP", "PER", "WS"]]
        .rename(columns=str.lower)          # lowercase to match SQL columns
        .sort_values("player")
    )

    for col in ["bpm", "vorp", "per", "ws"]:
        bbref_clean[col] = pd.to_numeric(bbref_clean[col], errors="coerce")

    bbref_clean.dropna(subset=["player"], inplace=True)
    return bbref_clean

# ── fetch salaries ────────────────────────────────────────────────────────────

def fetch_salaries():
    url = "https://www.basketball-reference.com/contracts/players.html"
    salary_df = pd.read_html(url)[0]

    salary_df.columns = salary_df.columns.droplevel(0)
    salary_df = salary_df[salary_df["Player"] != "Player"].copy()
    salary_df = (
        salary_df[["Player", "2025-26"]]
        .rename(columns={"Player": "player", "2025-26": "salary"})
    )
    salary_df["salary"] = pd.to_numeric(
        salary_df["salary"].str.replace(r"[$,]", "", regex=True), errors="coerce"
    )
    salary_df.dropna(inplace=True)
    salary_df.reset_index(drop=True, inplace=True)
    return salary_df

# ── fetch minutes ─────────────────────────────────────────────────────────────

def fetch_minutes():
    league_stats = leaguedashplayerstats.LeagueDashPlayerStats(
        measure_type_detailed_defense="Advanced",
        per_mode_detailed="PerGame"
    )
    stat_df = league_stats.league_dash_player_stats.get_data_frame()
    minutes_df = (
        stat_df[["PLAYER_NAME", "MIN"]]
        .rename(columns={"PLAYER_NAME": "player_name", "MIN": "min"})
    )
    return minutes_df

# ── run pipeline ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Starting ingestion...")

    print(os.getenv("DATABASE_URL"))

    upsert(fetch_advanced_stats(), "advanced_stats", "player")
    upsert(fetch_salaries(),       "salaries",       "player")
    upsert(fetch_minutes(),        "minutes",        "player_name")

    print("Done ✓")