-- =============================================================================
-- NBA Player Financial Value Pipeline
-- schema.sql
-- Run this in the Supabase SQL editor to initialize or reset all tables.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- RAW TABLES (populated by ingest.py)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS advanced_stats (
    id          SERIAL PRIMARY KEY,
    player      TEXT    NOT NULL,
    player_id   TEXT,                   -- BBREF player ID e.g. curryst01
    bpm         NUMERIC,                -- Box Plus/Minus
    vorp        NUMERIC,                -- Value Over Replacement Player
    per         NUMERIC,                -- Player Efficiency Rating
    ws          NUMERIC,                -- Win Shares
    updated_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS salaries (
    id          SERIAL PRIMARY KEY,
    player      TEXT    NOT NULL,
    salary      NUMERIC,                -- 2025-26 salary in USD
    updated_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS minutes (
    id          SERIAL PRIMARY KEY,
    player_name TEXT    NOT NULL,
    min         NUMERIC,                -- minutes per game
    updated_at  TIMESTAMP DEFAULT NOW()
);


-- -----------------------------------------------------------------------------
-- TRANSFORMED TABLE (populated by transform.py)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS player_metrics (
    id                   SERIAL PRIMARY KEY,
    player               TEXT    NOT NULL,
    player_id            TEXT,                   -- BBREF player ID e.g. curryst01
    salary               NUMERIC,                -- 2025-26 salary in USD
    min                  NUMERIC,                -- minutes per game
    vorp                 NUMERIC,                -- Value Over Replacement Player
    bpm                  NUMERIC,                -- Box Plus/Minus
    per                  NUMERIC,                -- Player Efficiency Rating
    ws                   NUMERIC,                -- Win Shares
    vorp_per_dollar      NUMERIC,                -- VORP / salary
    ws_per_dollar        NUMERIC,                -- WS / salary
    vorp_per_dollar_rank INTEGER,                -- rank among all qualified players
    ws_per_dollar_rank   INTEGER,                -- rank among all qualified players
    overall_value_rank   INTEGER,                -- avg of vorp/$ and ws/$ ranks, re-ranked
    updated_at           TIMESTAMP DEFAULT NOW()
);


-- -----------------------------------------------------------------------------
-- NOTES
-- -----------------------------------------------------------------------------
-- Qualified players: minimum 10 minutes per game (filtered in transform.py)
-- Ranks: 1 = best value. overall_value_rank averages vorp_per_dollar_rank
--        and ws_per_dollar_rank, then re-ranks that average.
-- Headshots: https://www.basketball-reference.com/req/202106291/images/headshots/{player_id}.jpg
-- -----------------------------------------------------------------------------