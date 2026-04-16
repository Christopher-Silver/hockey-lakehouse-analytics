# ============================================================
# gold_01_build_facts_from_events.py
#
# Builds the core curated fact tables for the Gold layer from
# dbw_hockey_lakehouse.silver.events.
#
# Output tables:
#   - dbw_hockey_lakehouse.gold.fact_shot_attempt
#   - dbw_hockey_lakehouse.gold.fact_penalty
#   - dbw_hockey_lakehouse.gold.fact_hit
#
# What this script is doing:
#   - filters event-level data into separate fact domains
#   - standardizes player roles for downstream joins and reporting
#   - keeps only the columns needed for analytics / BI use cases
#   - adds a few simple derived flags used heavily in KPI logic
#
# Write pattern:
#   - overwrite
#   - effectively idempotent, since these facts are rebuilt from
#     the current state of silver.events each run
#
# Notes:
#   - this is a foundation Gold modeling file, so the schemas here
#     are intentionally curated and fairly narrow
#   - role columns are renamed on purpose so downstream logic does
#     not have to keep re-interpreting generic player fields
# ============================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# ---------------- CONFIG ----------------

SILVER_EVENTS = "dbw_hockey_lakehouse.silver.events"

FACT_SHOT_ATTEMPT = "dbw_hockey_lakehouse.gold.fact_shot_attempt"
FACT_PENALTY = "dbw_hockey_lakehouse.gold.fact_penalty"
FACT_HIT = "dbw_hockey_lakehouse.gold.fact_hit"

SHOT_EVENT_TYPES = ["shot-on-goal", "missed-shot", "blocked-shot", "goal"]
PENALTY_EVENT_TYPES = ["penalty"]
HIT_EVENT_TYPES = ["hit"]


# ---------------- HELPERS ----------------

def overwrite_table(table_name: str, df: DataFrame) -> None:
    """Fully replaces a Delta table and refreshes the schema."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def preview_table(table_name: str, n: int = 10) -> None:
    # quick sanity check after each write
    display(
        spark.table(table_name)
        .orderBy(F.col("game_id").desc(), F.col("sort_order").desc())
        .limit(n)
    )


# ---------------- LOAD DATA ----------------

events_df = spark.table(SILVER_EVENTS)

# Keep this defensive check here just in case silver changes upstream.
# Not perfect, but it avoids breaking the Gold build on a simple casing issue.
if "event_type_lc" not in events_df.columns:
    events_df = events_df.withColumn("event_type_lc", F.lower(F.col("event_type")))


# ---------------- BUILD FACT: SHOT ATTEMPTS ----------------

shot_df = (
    events_df
    .filter(F.col("event_type_lc").isin([event.lower() for event in SHOT_EVENT_TYPES]))
    # These flags get reused downstream a lot, especially for KPIs and report logic.
    # Keeping them here avoids repeating the same case logic in Gold summaries / BI.
    .withColumn("is_goal", F.when(F.col("event_type_lc") == "goal", 1).otherwise(0))
    .withColumn(
        "is_sog",
        F.when(F.col("event_type_lc").isin("goal", "shot-on-goal"), 1).otherwise(0),
    )
    .withColumn("is_miss", F.when(F.col("event_type_lc") == "missed-shot", 1).otherwise(0))
    .withColumn("is_blocked", F.when(F.col("event_type_lc") == "blocked-shot", 1).otherwise(0))
    .select(
        # Curated schema only:
        # the goal here is to keep this fact table focused and analytics-ready,
        # not to pass every raw event column through just because we can.
        "season",
        "game_id",
        "event_id",
        "sort_order",
        "event_type",
        "type_code",
        "situation_code",
        "home_team_defending_side",
        "period_number",
        "period_type",
        "time_in_period",
        "time_remaining",
        "home_team_id",
        "away_team_id",
        "event_owner_team_id",

        # Standardized player roles. Silver keeps more generic role fields,
        # but Gold should be a little more explicit.
        F.col("primary_player_id").alias("shooter_player_id"),
        F.col("goalie_id").alias("goalie_player_id"),

        "x",
        "y",
        "zone_code",
        "shot_type",
        "reason",  # useful for misses / blocks and some downstream shot detail views
        "home_score",
        "away_score",

        # Shot outcome flags
        # is_goal: scored shot
        # is_sog: official shot on goal, including goals
        # is_miss: missed the net / no shot on goal recorded
        # is_blocked: attempt blocked before reaching the net
        "is_goal",
        "is_sog",
        "is_miss",
        "is_blocked",

        "source_url",
        "ingested_at",
    )
)

overwrite_table(FACT_SHOT_ATTEMPT, shot_df)
print(f"Overwrote: {FACT_SHOT_ATTEMPT} | rows = {spark.table(FACT_SHOT_ATTEMPT).count()}")
preview_table(FACT_SHOT_ATTEMPT, 10)


# ---------------- BUILD FACT: PENALTIES ----------------

penalty_df = (
    events_df
    .filter(F.col("event_type_lc").isin([event.lower() for event in PENALTY_EVENT_TYPES]))
    .withColumn("penalty_is_major", F.when(F.col("penalty_minutes") >= 5, 1).otherwise(0))
    .withColumn("penalty_is_minor", F.when(F.col("penalty_minutes") == 2, 1).otherwise(0))
    .select(
        # Same idea here: curated fact schema, not a dump of the full event record.
        "season",
        "game_id",
        "event_id",
        "sort_order",
        "event_type",
        "type_code",
        "situation_code",
        "home_team_defending_side",
        "period_number",
        "period_type",
        "time_in_period",
        "time_remaining",
        "home_team_id",
        "away_team_id",
        "event_owner_team_id",

        # Standardized player roles for penalty events
        F.col("primary_player_id").alias("penalized_player_id"),
        F.col("secondary_player_id").alias("drawn_by_player_id"),

        "x",
        "y",
        "zone_code",
        "penalty_type_code",
        "penalty_desc_key",
        "penalty_minutes",
        "penalty_is_major",
        "penalty_is_minor",
        "home_score",
        "away_score",
        "source_url",
        "ingested_at",
    )
)

overwrite_table(FACT_PENALTY, penalty_df)
print(f"Overwrote: {FACT_PENALTY} | rows = {spark.table(FACT_PENALTY).count()}")
preview_table(FACT_PENALTY, 10)


# ---------------- BUILD FACT: HITS ----------------

hit_df = (
    events_df
    .filter(F.col("event_type_lc").isin([event.lower() for event in HIT_EVENT_TYPES]))
    .select(
        "season",
        "game_id",
        "event_id",
        "sort_order",
        "event_type",
        "type_code",
        "situation_code",
        "home_team_defending_side",
        "period_number",
        "period_type",
        "time_in_period",
        "time_remaining",
        "home_team_id",
        "away_team_id",
        "event_owner_team_id",

        # Standardized hit roles so downstream joins stay simple
        F.col("primary_player_id").alias("hitter_player_id"),
        F.col("secondary_player_id").alias("hittee_player_id"),

        "x",
        "y",
        "zone_code",
        "home_score",
        "away_score",
        "source_url",
        "ingested_at",
    )
)

overwrite_table(FACT_HIT, hit_df)
print(f"Overwrote: {FACT_HIT} | rows = {spark.table(FACT_HIT).count()}")
preview_table(FACT_HIT, 10)

print("Gold fact build complete.")