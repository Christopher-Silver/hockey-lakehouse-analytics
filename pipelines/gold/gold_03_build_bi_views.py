# ============================================================
# gold_03_build_bi_views.py
#
# Creates BI-friendly serving views on top of the Gold fact and KPI tables.
#
# Output schema:
#   - dbw_hockey_lakehouse.gold_bi
#
# Views created:
#   - v_fact_shot_attempt
#   - v_fact_penalty
#   - v_fact_hit
#   - v_kpi_goalie_location_grid
#   - v_kpi_penalties_taken_drawn
#   - v_kpi_blocks_vs_sog
#   - v_kpi_player_shot_miss_map
#   - v_kpi_hits_60_by_state
#   - v_kpi_goalie_location_grid_league
#
# What this script is doing:
#   - exposes Gold data through cleaner, BI-friendly views
#   - adds readable labels for common coded fields
#   - parses owner_strength_state into reporting-friendly columns
#   - keeps dashboard-facing logic centralized so analysts do not have to
#     rebuild the same transformations in Power BI
#
# Why views instead of tables:
#   - these are serving-layer semantics, not new source-of-truth datasets
#   - views keep the storage footprint down
#   - if the underlying Gold tables refresh, the BI layer stays aligned
#
# Write pattern:
#   - create or replace views
#   - idempotent
# ============================================================

# ---------------- DATABASE ----------------

BI_DATABASE = "dbw_hockey_lakehouse.gold_bi"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BI_DATABASE}")


# ---------------- SOURCES ----------------

FACT_SHOT_ATTEMPT = "dbw_hockey_lakehouse.gold.fact_shot_attempt"
FACT_PENALTY = "dbw_hockey_lakehouse.gold.fact_penalty"
FACT_HIT = "dbw_hockey_lakehouse.gold.fact_hit"

KPI_GOALIE_GRID = "dbw_hockey_lakehouse.gold.kpi_goalie_location_grid"
KPI_PEN_TAKEDRAW = "dbw_hockey_lakehouse.gold.kpi_penalties_taken_drawn"
KPI_BLOCKS_VS_SOG = "dbw_hockey_lakehouse.gold.kpi_blocks_vs_sog"
KPI_MISS_MAP = "dbw_hockey_lakehouse.gold.kpi_player_shot_miss_map"
KPI_HITS_60 = "dbw_hockey_lakehouse.gold.kpi_hits_60_by_state"


# ---------------- SHARED SQL FRAGMENTS ----------------

# Zone labels are added here so dashboard users do not have to remember
# what O / D / N means or recreate that mapping in every report.
ZONE_LABEL_CASE = """
CASE
  WHEN zone_code = 'O' THEN 'Offensive'
  WHEN zone_code = 'D' THEN 'Defensive'
  WHEN zone_code = 'N' THEN 'Neutral'
  ELSE 'Unknown'
END
"""

# owner_strength_state is great for storage and modeling, but not ideal for BI as-is.
# Parsing it once here makes filtering and labeling much easier in Power BI.
#
# Examples:
#   EV_5v5
#   PP_5v4
#   SH_4v5
#   EN_EV_6v5
OWNER_STRENGTH_BASE_CTE = """
SELECT
  *,
  CASE
    WHEN owner_strength_state LIKE 'EN\\_%' THEN true
    ELSE false
  END AS empty_net_flag,
  CASE
    WHEN owner_strength_state LIKE 'EN\\_%' THEN split(substring(owner_strength_state, 4), '_')[0]
    ELSE split(owner_strength_state, '_')[0]
  END AS state,
  CASE
    WHEN owner_strength_state LIKE 'EN\\_%' THEN split(split(substring(owner_strength_state, 4), '_')[1], 'v')[0]
    ELSE split(split(owner_strength_state, '_')[1], 'v')[0]
  END AS owner_skaters,
  CASE
    WHEN owner_strength_state LIKE 'EN\\_%' THEN split(split(substring(owner_strength_state, 4), '_')[1], 'v')[1]
    ELSE split(split(owner_strength_state, '_')[1], 'v')[1]
  END AS opp_skaters
"""

STATE_LABEL_CASE = """
CASE
  WHEN state = 'EV' THEN 'Even Strength'
  WHEN state = 'PP' THEN 'Power Play'
  WHEN state = 'SH' THEN 'Short Handed'
  ELSE 'Other'
END
"""


# ---------------- BI VIEW: FACT SHOT ATTEMPT ----------------

spark.sql(f"""
CREATE OR REPLACE VIEW {BI_DATABASE}.v_fact_shot_attempt AS
SELECT
  season,
  game_id,
  event_id,
  sort_order,
  event_type,
  type_code,
  situation_code,
  home_team_defending_side,
  period_number,
  period_type,
  time_in_period,
  time_remaining,
  home_team_id,
  away_team_id,
  event_owner_team_id AS team_id,
  shooter_player_id AS player_id,
  goalie_player_id,
  x,
  y,
  zone_code,
  {ZONE_LABEL_CASE} AS zone_label,
  shot_type,
  reason,
  home_score,
  away_score,
  is_goal,
  is_sog,
  is_miss,
  is_blocked,
  source_url,
  ingested_at
FROM {FACT_SHOT_ATTEMPT}
""")


# ---------------- BI VIEW: FACT PENALTY ----------------

spark.sql(f"""
CREATE OR REPLACE VIEW {BI_DATABASE}.v_fact_penalty AS
SELECT
  season,
  game_id,
  event_id,
  sort_order,
  event_type,
  type_code,
  situation_code,
  home_team_defending_side,
  period_number,
  period_type,
  time_in_period,
  time_remaining,
  home_team_id,
  away_team_id,
  event_owner_team_id AS team_id,
  penalized_player_id,
  drawn_by_player_id,
  x,
  y,
  zone_code,
  {ZONE_LABEL_CASE} AS zone_label,
  penalty_type_code,
  penalty_desc_key,
  penalty_minutes,
  penalty_is_major,
  penalty_is_minor,
  home_score,
  away_score,
  source_url,
  ingested_at
FROM {FACT_PENALTY}
""")


# ---------------- BI VIEW: FACT HIT ----------------

spark.sql(f"""
CREATE OR REPLACE VIEW {BI_DATABASE}.v_fact_hit AS
SELECT
  season,
  game_id,
  event_id,
  sort_order,
  event_type,
  type_code,
  situation_code,
  home_team_defending_side,
  period_number,
  period_type,
  time_in_period,
  time_remaining,
  home_team_id,
  away_team_id,
  event_owner_team_id AS team_id,
  hitter_player_id,
  hittee_player_id,
  x,
  y,
  zone_code,
  {ZONE_LABEL_CASE} AS zone_label,
  home_score,
  away_score,
  source_url,
  ingested_at
FROM {FACT_HIT}
""")


# ---------------- BI VIEW: KPI GOALIE LOCATION GRID ----------------

spark.sql(f"""
CREATE OR REPLACE VIEW {BI_DATABASE}.v_kpi_goalie_location_grid AS
SELECT
  season,
  goalie_player_id AS player_id,
  x_bin,
  y_bin,
  rink_x,
  rink_y,
  viz_rink_x,
  viz_rink_y,
  shots,
  goals,
  CASE
    WHEN shots > 0 THEN goals / shots
    ELSE NULL
  END AS goal_rate
FROM {KPI_GOALIE_GRID}
""")


# ---------------- BI VIEW: KPI PENALTIES TAKEN / DRAWN ----------------

spark.sql(f"""
CREATE OR REPLACE VIEW {BI_DATABASE}.v_kpi_penalties_taken_drawn AS
WITH base AS (
  {OWNER_STRENGTH_BASE_CTE}
  FROM {KPI_PEN_TAKEDRAW}
)
SELECT
  season,
  entity_type,
  entity_id,
  role,
  owner_strength_state,
  state,
  {STATE_LABEL_CASE} AS state_label,
  empty_net_flag,
  CAST(owner_skaters AS INT) AS owner_skaters,
  CAST(opp_skaters AS INT) AS opp_skaters,
  zone_code,
  {ZONE_LABEL_CASE} AS zone_label,
  penalty_type_code,
  penalty_minutes,
  penalties
FROM base
""")


# ---------------- BI VIEW: KPI BLOCKS VS SOG ----------------

spark.sql(f"""
CREATE OR REPLACE VIEW {BI_DATABASE}.v_kpi_blocks_vs_sog AS
WITH base AS (
  {OWNER_STRENGTH_BASE_CTE}
  FROM {KPI_BLOCKS_VS_SOG}
)
SELECT
  season,
  entity_type,
  entity_id,
  owner_strength_state,
  state,
  {STATE_LABEL_CASE} AS state_label,
  empty_net_flag,
  CAST(owner_skaters AS INT) AS owner_skaters,
  CAST(opp_skaters AS INT) AS opp_skaters,
  blocks,
  sog,
  CASE
    WHEN sog > 0 THEN blocks / sog
    ELSE NULL
  END AS blocks_per_sog
FROM base
""")


# ---------------- BI VIEW: KPI PLAYER SHOT MISS MAP ----------------

spark.sql(f"""
CREATE OR REPLACE VIEW {BI_DATABASE}.v_kpi_player_shot_miss_map AS
SELECT
  season,
  player_id,
  x_bin,
  y_bin,
  rink_x,
  rink_y,
  viz_rink_x,
  viz_rink_y,
  shot_type,
  attempts,
  misses,
  miss_rate
FROM {KPI_MISS_MAP}
""")


# ---------------- BI VIEW: KPI HITS PER 60 BY STATE ----------------

spark.sql(f"""
CREATE OR REPLACE VIEW {BI_DATABASE}.v_kpi_hits_60_by_state AS
WITH base AS (
  {OWNER_STRENGTH_BASE_CTE}
  FROM {KPI_HITS_60}
)
SELECT
  season,
  team_id,
  owner_strength_state,
  state,
  {STATE_LABEL_CASE} AS state_label,
  empty_net_flag,
  CAST(owner_skaters AS INT) AS owner_skaters,
  CAST(opp_skaters AS INT) AS opp_skaters,
  hits,
  proxy_seconds,
  hits_per_60
FROM base
""")


# ---------------- BI VIEW: KPI GOALIE LOCATION GRID LEAGUE ----------------

# League baseline view at season x rink cell grain.
# Useful for comparisons in BI without forcing the report layer to re-aggregate goalie rows.
spark.sql(f"""
CREATE OR REPLACE VIEW {BI_DATABASE}.v_kpi_goalie_location_grid_league AS
SELECT
  season,
  rink_x,
  rink_y,
  MAX(viz_rink_x) AS viz_rink_x,
  MAX(viz_rink_y) AS viz_rink_y,
  SUM(shots) AS league_shots,
  SUM(goals) AS league_goals,
  CASE
    WHEN SUM(shots) = 0 THEN NULL
    ELSE SUM(goals) / SUM(shots)
  END AS league_goal_rate
FROM {KPI_GOALIE_GRID}
GROUP BY
  season,
  rink_x,
  rink_y
""")


print(f"BI views created in {BI_DATABASE}")
display(spark.sql(f"SHOW TABLES IN {BI_DATABASE}"))