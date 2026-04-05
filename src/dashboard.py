import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# ── connection ────────────────────────────────────────────────────────────────

def get_engine():
    return create_engine(st.secrets["DATABASE_URL"])

@st.cache_data(ttl=3600)
def load_metrics():
    engine = get_engine()
    return pd.read_sql("SELECT * FROM player_metrics ORDER BY overall_value_rank", engine)

# ── page config ───────────────────────────────────────────────────────────────

st.set_page_config(page_title="NBA Player Value", layout="wide")
st.title("NBA Player Financial Value Dashboard")

# ── load data ─────────────────────────────────────────────────────────────────

df = load_metrics()

# ── player selector ───────────────────────────────────────────────────────────

player = st.selectbox(
    "Select a player",
    options=sorted(df["player"].unique())
)

player_row = df[df["player"] == player].iloc[0]
player_min = player_row["min"]

player_id = player_row["player_id"]
headshot_url = f"https://www.basketball-reference.com/req/202106291/images/headshots/{player_id}.jpg"

st.image(headshot_url)

st.divider()

# ── table 1: advanced stats ───────────────────────────────────────────────────

st.subheader("Advanced Stats")

advanced = pd.DataFrame({
    "MINUTES PER GAME":  [player_row["min"]],
    "PER":  [player_row["per"]],
    "BPM":  [player_row["bpm"]],
    "VORP": [player_row["vorp"]],
    "WS":   [player_row["ws"]]
}).round(2)

st.dataframe(advanced, hide_index=True, width="stretch")

# ── table 2: financial value ──────────────────────────────────────────────────

st.subheader("Financial Value")

total_global = len(df)

value = pd.DataFrame({
    "Annual Salary":    [f"${player_row['salary']:,.0f}"],
    "VORP / Dollar":    [round(player_row["vorp_per_dollar"], 8)],
    "WS / Dollar":      [round(player_row["ws_per_dollar"], 8)],
    "VORP/$ Rank":      [f"{int(player_row['vorp_per_dollar_rank'])} of {total_global}"],
    "WS/$ Rank":        [f"{int(player_row['ws_per_dollar_rank'])} of {total_global}"],
    "Overall Value Rank":   [f"{int(player_row['overall_value_rank'])} of {total_global}"],
})

st.dataframe(
    value,
    hide_index=True,
    width="stretch",
    column_config={
        "VORP/$ Rank":        st.column_config.TextColumn(width="small"),
        "WS/$ Rank":          st.column_config.TextColumn(width="small"),
        "Overall Value Rank": st.column_config.TextColumn(width="small"),
    }
)
st.caption("Overall Value Rank is computed by averaging a player's VORP/\\$ rank and WS/\\$ rank across all qualified players (min. 10 minutes per game), then re-ranking that average.")

# ── table 3: similar minutes comparison ──────────────────────────────────────

st.subheader("Comparison: Similar Minutes Players")

minute_delta = st.slider(
    "Minutes per game window (± from selected player)",
    min_value=1, max_value=10, value=3
)

# Filter comparison group
comp = df[
    (df["min"] >= player_min - minute_delta) &
    (df["min"] <= player_min + minute_delta) &
    (df["player"] != player)
].copy()

# Include selected player to rank within the full group
comp_with_selected = pd.concat([df[df["player"] == player], comp], ignore_index=True)

comp_with_selected["local_vorp_rank"]  = comp_with_selected["vorp_per_dollar"].rank(ascending=False).astype(int)
comp_with_selected["local_ws_rank"]    = comp_with_selected["ws_per_dollar"].rank(ascending=False).astype(int)
comp_with_selected["local_value_rank"] = (
    (comp_with_selected["local_vorp_rank"] + comp_with_selected["local_ws_rank"]) / 2
).rank(ascending=True).astype(int)

selected_local = comp_with_selected[comp_with_selected["player"] == player].iloc[0]
comp_local     = comp_with_selected[comp_with_selected["player"] != player].copy()

# Selected player row
total_in_group = len(comp_with_selected)
local_rank = int(selected_local["local_value_rank"])
global_rank = int(selected_local["overall_value_rank"])
total_global = len(df)

league_percentile = round((1 - (global_rank - 1) / total_global) * 100, 1)
group_percentile  = round((1 - (local_rank - 1) / total_in_group) * 100, 1)

selected_display = pd.DataFrame([{
    "Player":                    selected_local["player"],
    "MIN":                       selected_local["min"],
    "Salary":                    f"${selected_local['salary']:,.0f}",
    "VORP":                      round(selected_local["vorp"], 2),
    "WS":                        round(selected_local["ws"], 2),
    "BPM":                       round(selected_local["bpm"], 2),
    "PER":                       round(selected_local["per"], 2),
    "VORP/$":                    round(selected_local["vorp_per_dollar"], 2),
    "WS/$":                      round(selected_local["ws_per_dollar"], 2),
    "Group Rank":                f"{local_rank} of {total_in_group}",
    "Group Percentile":          f"Top {group_percentile}%",
    "League Percentile":         f"Top {league_percentile}%",
}])

st.dataframe(
    selected_display,
    hide_index=True,
    width="stretch",
    column_config={
        "Local Value Rank": st.column_config.TextColumn(width="small"),
        "Group Percentile": st.column_config.TextColumn(width="small"),
        "League Percentile": st.column_config.TextColumn(width="small"),
    }
)

# Comparison table
comp_display = comp_local[[
    "player", "min", "salary", "vorp", "ws", "bpm", "per",
    "vorp_per_dollar", "ws_per_dollar", "local_value_rank"
]].rename(columns={
    "player":           "Player",
    "min":              "MIN",
    "salary":           "Salary",
    "vorp":             "VORP",
    "ws":               "WS",
    "bpm":              "BPM",
    "per":              "PER",
    "vorp_per_dollar":  "VORP/$",
    "ws_per_dollar":    "WS/$",
    "local_value_rank": "Local Value Rank",
}).sort_values("Local Value Rank").round(2)

comp_display["Salary"] = comp_display["Salary"].apply(lambda x: f"${x:,.0f}")

st.dataframe(comp_display, hide_index=True, width="stretch")

st.caption("All statistics are for the current NBA season. Data updated daily at 1am PST")