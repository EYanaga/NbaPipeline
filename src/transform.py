import pandas as pd
import os
import unicodedata
import re
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))

def normalize_name(name: str) -> str:
    name = unicodedata.normalize("NFKD", str(name))
    name = name.encode("ascii", "ignore").decode("ascii")
    name = re.sub(r"\s+(Jr\.?|Sr\.?|II|III|IV)$", "", name.strip())
    return name.lower()

def load_tables():
    advanced = pd.read_sql("SELECT * FROM advanced_stats", engine)
    salaries = pd.read_sql("SELECT * FROM salaries", engine)
    minutes  = pd.read_sql("SELECT * FROM minutes", engine)
    return advanced, salaries, minutes

def check_unmatched(advanced, salaries, minutes):
    adv_names = set(advanced["player_normalized"])
    sal_names = set(salaries["player_normalized"])
    min_names = set(minutes["player_normalized"])

    no_salary  = adv_names - sal_names
    no_minutes = adv_names - min_names

    print(f"\nIn advanced but no salary match ({len(no_salary)}):")
    for p in sorted(no_salary)[:20]:
        print(f"  {p}")

    print(f"\nIn advanced but no minutes match ({len(no_minutes)}):")
    for p in sorted(no_minutes)[:20]:
        print(f"  {p}")

def compute_metrics(advanced, salaries, minutes):
    # Merge on normalized name, keep original name for display
    df = advanced.merge(salaries, on="player_normalized", how="inner", suffixes=("", "_sal"))
    df = df.merge(minutes, on="player_normalized", how="inner", suffixes=("", "_min"))

    # Filter to meaningful sample
    df = df[df["min"] >= 10].copy()

    # Core value metrics
    df["vorp_per_dollar"] = df["vorp"] / df["salary"]
    df["ws_per_dollar"]   = df["ws"]   / df["salary"]

    # Individual ranks (1 = best)
    df["vorp_per_dollar_rank"] = df["vorp_per_dollar"].rank(ascending=False).astype(int)
    df["ws_per_dollar_rank"]   = df["ws_per_dollar"].rank(ascending=False).astype(int)

    # Combined rank — average of the two rank columns
    df["overall_value_rank"] = (
        (df["vorp_per_dollar_rank"] + df["ws_per_dollar_rank"]) / 2
    ).rank(ascending=True).astype(int)

    return df[[
        "player", "salary", "min", "vorp", "bpm", "per", "ws",
        "vorp_per_dollar", "ws_per_dollar",
        "vorp_per_dollar_rank", "ws_per_dollar_rank", "overall_value_rank"
    ]]

def save_metrics(df):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM player_metrics"))
    df.to_sql("player_metrics", engine, if_exists="append", index=False)
    print(f"✓ player_metrics: {len(df)} rows written")

if __name__ == "__main__":
    print("Starting transform...")
    advanced, salaries, minutes = load_tables()

    # Normalize names for matching
    for df, col in [(advanced, "player"), (salaries, "player"), (minutes, "player_name")]:
        df["player_normalized"] = df[col].apply(normalize_name)
    # minutes uses player_name, align it
    minutes = minutes.rename(columns={"player_name": "player"})

    check_unmatched(advanced, salaries, minutes)  # remove once you're happy with matches

    metrics = compute_metrics(advanced, salaries, minutes)
    save_metrics(metrics)
    print("Done ✓")