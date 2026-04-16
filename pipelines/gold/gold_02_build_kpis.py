# ============================================================
# gold_02_build_kpis.py
#
# Builds curated KPI tables from the Gold fact layer.
#
# Output tables:
#   - dbw_hockey_lakehouse.gold.kpi_goalie_location_grid
#   - dbw_hockey_lakehouse.gold.kpi_penalties_taken_drawn
#   - dbw_hockey_lakehouse.gold.kpi_blocks_vs_sog
#   - dbw_hockey_lakehouse.gold.kpi_player_shot_miss_map
#   - dbw_hockey_lakehouse.gold.kpi_hits_60_by_state
#
# Source tables:
#   - dbw_hockey_lakehouse.gold.fact_shot_attempt
#   - dbw_hockey_lakehouse.gold.fact_penalty
#   - dbw_hockey_lakehouse.gold.fact_hit
#
# What this script is doing:
#   - derives owner-perspective strength state from situation_code
#   - builds spatial bins for shot-based location KPIs
#   - folds full-rink coordinates into a half-rink view for cleaner reporting
#   - creates a handful of analytics-ready KPI tables used in Power BI
#
# Write pattern:
#   - overwrite
#   - effectively idempotent, since everything is rebuilt from curated fact tables
#
# Notes:
#   - situation_code is doing a lot of work here
#   - the rink-folding logic is intentional and is kept separate from the raw bins
#     so analytics fields stay stable while viz fields can be adjusted independently
# ============================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# ---------------- CONFIG ----------------

FACT_SHOT_ATTEMPT = "dbw_hockey_lakehouse.gold.fact_shot_attempt"
FACT_PENALTY = "dbw_hockey_lakehouse.gold.fact_penalty"
FACT_HIT = "dbw_hockey_lakehouse.gold.fact_hit"

KPI_GOALIE_GRID = "dbw_hockey_lakehouse.gold.kpi_goalie_location_grid"
KPI_PEN_TAKEDRAW = "dbw_hockey_lakehouse.gold.kpi_penalties_taken_drawn"
KPI_BLOCKS_VS_SOG = "dbw_hockey_lakehouse.gold.kpi_blocks_vs_sog"
KPI_MISS_MAP = "dbw_hockey_lakehouse.gold.kpi_player_shot_miss_map"
KPI_HITS_60 = "dbw_hockey_lakehouse.gold.kpi_hits_60_by_state"

# Spatial binning settings for rink-based KPIs.
# Kept explicit here because these choices affect every downstream map.
X_MIN, X_MAX, X_BIN_W = -100.0, 100.0, 5.0
Y_MIN, Y_MAX, Y_BIN_W = -42.5, 42.5, 5.0


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


def add_xy_bins(df: DataFrame, x_col: str = "x", y_col: str = "y") -> DataFrame:
    """
    Adds integer x/y bins for rink-based analysis.

    We clip coordinates before binning so weird edge values from the source
    do not create broken bins or stretch the map unexpectedly.
    """
    x_clip = (
        F.when(F.col(x_col) < F.lit(X_MIN), F.lit(X_MIN))
        .when(F.col(x_col) > F.lit(X_MAX), F.lit(X_MAX))
        .otherwise(F.col(x_col))
    )

    y_clip = (
        F.when(F.col(y_col) < F.lit(Y_MIN), F.lit(Y_MIN))
        .when(F.col(y_col) > F.lit(Y_MAX), F.lit(Y_MAX))
        .otherwise(F.col(y_col))
    )

    return (
        df
        .withColumn("_x_clip", x_clip.cast("double"))
        .withColumn("_y_clip", y_clip.cast("double"))
        .withColumn(
            "x_bin",
            F.floor((F.col("_x_clip") - F.lit(X_MIN)) / F.lit(X_BIN_W)).cast("int"),
        )
        .withColumn(
            "y_bin",
            F.floor((F.col("_y_clip") - F.lit(Y_MIN)) / F.lit(Y_BIN_W)).cast("int"),
        )
        .drop("_x_clip", "_y_clip")
    )


# ---------------- RINK FOLDING HELPERS ----------------

def compute_bin_limits(df_binned: DataFrame) -> tuple[int, int, int]:
    """
    Returns (max_x_bin, max_y_bin, center_x_bin) based on the data present.

    I prefer using observed limits instead of hard-coding theoretical ones here.
    Makes the folding logic a little safer if the source shape shifts.
    """
    row = (
        df_binned
        .agg(
            F.max("x_bin").alias("max_x_bin"),
            F.max("y_bin").alias("max_y_bin"),
        )
        .collect()[0]
    )

    max_x_bin = int(row["max_x_bin"])
    max_y_bin = int(row["max_y_bin"])
    center_x_bin = max_x_bin // 2

    return max_x_bin, max_y_bin, center_x_bin


def add_rink_xy_from_bins(
    df: DataFrame,
    max_x_bin: int,
    max_y_bin: int,
    center_x_bin: int,
) -> DataFrame:
    """
    Adds folded half-rink coordinates from the original x/y bins.

    Output:
      - rink_y: distance from center line toward the attacking end
      - rink_x: lateral position across the rink after folding

    The important part here is that we do NOT overwrite x_bin / y_bin.
    Raw-ish analytic bins stay intact, then we add folded rink coordinates
    specifically for reporting and heatmap-style visuals.
    """
    flip_far_half = F.col("x_bin") > F.lit(center_x_bin)

    # Fold the far half of the rink inward so both attacking directions
    # land on the same half-rink view.
    x_fold = F.when(flip_far_half, F.lit(max_x_bin) - F.col("x_bin")).otherwise(F.col("x_bin"))

    # When x is folded, y also needs to flip or the left/right handedness
    # ends up backwards on one half. This part is easy to miss.
    y_fold = F.when(flip_far_half, F.lit(max_y_bin) - F.col("y_bin")).otherwise(F.col("y_bin"))

    return (
        df
        .withColumn("rink_y", x_fold.cast("int"))
        .withColumn("rink_x", y_fold.cast("int"))
    )


def add_viz_rink_xy(df: DataFrame, max_rink_x: int, max_rink_y: int) -> DataFrame:
    """
    Adds viz_rink_x / viz_rink_y for plotting only.

    These are slightly nudged versions of rink_x / rink_y to improve the look
    of heatmaps near the boundaries. Analytics columns stay untouched.
    """
    rx = F.col("rink_x").cast("double")
    ry = F.col("rink_y").cast("double")

    viz_rx = (
        F.when(F.col("rink_x") == 0, F.lit(0.5))
        .when(F.col("rink_x") == max_rink_x, F.lit(float(max_rink_x) - 0.5))
        .otherwise(rx)
    )

    viz_ry = (
        F.when(F.col("rink_y") == 0, F.lit(0.5))
        .when(F.col("rink_y") == 2, F.lit(2.5))  # goal-line clamp idea
        .when(F.col("rink_y") == max_rink_y, F.lit(float(max_rink_y) - 0.5))
        .otherwise(ry)
    )

    return (
        df
        .withColumn("viz_rink_x", viz_rx)
        .withColumn("viz_rink_y", viz_ry)
    )


def time_in_period_to_seconds(col) -> F.Column:
    """Converts a MM:SS clock string into elapsed seconds."""
    parts = F.split(col, ":")
    return (parts.getItem(0).cast("int") * 60 + parts.getItem(1).cast("int")).cast("int")


def add_owner_strength_state(df: DataFrame) -> DataFrame:
    """
    Parses situation_code and derives strength state from the event owner's perspective.

    situation_code format:
      [away goalie in net][away skaters][home skaters][home goalie in net]

    Example:
      1551
        - away goalie in net = 1
        - away skaters       = 5
        - home skaters       = 5
        - home goalie in net = 1

    This matters a lot because '5v4' alone is not enough for analysis.
    We need to know whether that advantage belongs to the event owner or the opponent,
    otherwise PP / SH splits get mislabeled downstream.
    """
    sc = F.lpad(F.col("situation_code").cast("string"), 4, "0")

    away_goalie_in_net = F.substring(sc, 1, 1).cast("int")
    away_skaters = F.substring(sc, 2, 1).cast("int")
    home_skaters = F.substring(sc, 3, 1).cast("int")
    home_goalie_in_net = F.substring(sc, 4, 1).cast("int")

    is_home_owner = F.col("event_owner_team_id") == F.col("home_team_id")

    # Strength state has to be calculated from the event owner's point of view.
    # Same raw situation_code can mean PP for one team and SH for the other.
    owner_advantage = (
        F.when(is_home_owner, home_skaters - away_skaters)
        .otherwise(away_skaters - home_skaters)
    )

    base_state = (
        F.when(owner_advantage > 0, F.lit("PP"))
        .when(owner_advantage < 0, F.lit("SH"))
        .otherwise(F.lit("EV"))
    )

    owner_skaters = F.when(is_home_owner, home_skaters).otherwise(away_skaters)
    opp_skaters = F.when(is_home_owner, away_skaters).otherwise(home_skaters)

    owner_goalie_in_net = F.when(is_home_owner, home_goalie_in_net).otherwise(away_goalie_in_net)
    opp_goalie_in_net = F.when(is_home_owner, away_goalie_in_net).otherwise(home_goalie_in_net)

    # Prefix empty-net situations so they do not quietly get mixed into standard states.
    # That distinction matters for both hockey interpretation and cleaner reporting.
    en_prefix = (
        F.when((owner_goalie_in_net == 0) | (opp_goalie_in_net == 0), F.lit("EN_"))
        .otherwise(F.lit(""))
    )

    owner_strength_state = F.concat(
        en_prefix,
        base_state,
        F.lit("_"),
        owner_skaters.cast("string"),
        F.lit("v"),
        opp_skaters.cast("string"),
    )

    return (
        df
        .withColumn("away_goalie_in_net", away_goalie_in_net)
        .withColumn("away_skaters", away_skaters)
        .withColumn("home_skaters", home_skaters)
        .withColumn("home_goalie_in_net", home_goalie_in_net)
        .withColumn("owner_strength_state", owner_strength_state)
    )


# ---------------- LOAD FACTS ----------------

shot = add_owner_strength_state(spark.table(FACT_SHOT_ATTEMPT))
pen = add_owner_strength_state(spark.table(FACT_PENALTY))
hit = add_owner_strength_state(spark.table(FACT_HIT))

# Pre-bin once so spatial KPIs reuse the same mapping logic.
# Small thing, but it keeps the notebook cleaner and avoids recomputing it everywhere.
shot_binned = shot.transform(lambda df: add_xy_bins(df, "x", "y"))

# Use actual observed bins from the data instead of assuming the rink extents
# always land exactly where the theoretical math says they should.
MAX_X_BIN, MAX_Y_BIN, CENTER_X_BIN = compute_bin_limits(shot_binned)

MAX_RINK_X = MAX_Y_BIN
MAX_RINK_Y = MAX_X_BIN // 2

print(
    "Bin limits:",
    {
        "MAX_X_BIN": MAX_X_BIN,
        "CENTER_X_BIN": CENTER_X_BIN,
        "MAX_Y_BIN": MAX_Y_BIN,
    },
)


# ---------------- KPI 1: GOALIE LOCATION GRID ----------------

# Heatmap-ready goalie shot locations.
# "shots" here means shots on goal faced, so goals are included.
goalie_grid = (
    shot_binned
    .filter(F.col("goalie_player_id").isNotNull())
    .filter(F.col("is_sog") == 1)
    .groupBy("season", F.col("goalie_player_id").alias("player_id"), "x_bin", "y_bin")
    .agg(
        F.count(F.lit(1)).alias("shots"),
        F.sum(F.col("is_goal")).alias("goals"),
    )
    .select(
        "season",
        F.col("player_id").alias("goalie_player_id"),
        "x_bin",
        "y_bin",
        "shots",
        "goals",
    )
    .transform(lambda df: add_rink_xy_from_bins(df, MAX_X_BIN, MAX_Y_BIN, CENTER_X_BIN))
    .transform(lambda df: add_viz_rink_xy(df, MAX_RINK_X, MAX_RINK_Y))
)

overwrite_table(KPI_GOALIE_GRID, goalie_grid)
print(f"Overwrote: {KPI_GOALIE_GRID} | rows = {spark.table(KPI_GOALIE_GRID).count()}")
display(spark.table(KPI_GOALIE_GRID).orderBy(F.col("shots").desc()).limit(10))


# ---------------- KPI 2: PENALTIES TAKEN VS DRAWN ----------------

# Player-level penalties taken
pen_taken_player = (
    pen
    .filter(F.col("penalized_player_id").isNotNull())
    .select(
        "season",
        F.lit("player").alias("entity_type"),
        F.col("penalized_player_id").alias("entity_id"),
        F.lit("taken").alias("role"),
        "owner_strength_state",
        "zone_code",
        "penalty_type_code",
        "penalty_minutes",
    )
)

# Player-level penalties drawn
pen_drawn_player = (
    pen
    .filter(F.col("drawn_by_player_id").isNotNull())
    .select(
        "season",
        F.lit("player").alias("entity_type"),
        F.col("drawn_by_player_id").alias("entity_id"),
        F.lit("drawn").alias("role"),
        "owner_strength_state",
        "zone_code",
        "penalty_type_code",
        "penalty_minutes",
    )
)

# Team-level penalties taken
# This stays useful even when the source does not attribute every penalty
# cleanly to an individual player.
pen_taken_team = (
    pen
    .filter(F.col("event_owner_team_id").isNotNull())
    .select(
        "season",
        F.lit("team").alias("entity_type"),
        F.col("event_owner_team_id").alias("entity_id"),
        F.lit("taken").alias("role"),
        "owner_strength_state",
        "zone_code",
        "penalty_type_code",
        "penalty_minutes",
    )
)

pen_kpi = (
    pen_taken_player
    .unionByName(pen_drawn_player)
    .unionByName(pen_taken_team)
    .groupBy(
        "season",
        "entity_type",
        "entity_id",
        "role",
        "owner_strength_state",
        "zone_code",
        "penalty_type_code",
        "penalty_minutes",
    )
    .agg(F.count(F.lit(1)).alias("penalties"))
)

overwrite_table(KPI_PEN_TAKEDRAW, pen_kpi)
print(f"Overwrote: {KPI_PEN_TAKEDRAW} | rows = {spark.table(KPI_PEN_TAKEDRAW).count()}")
display(spark.table(KPI_PEN_TAKEDRAW).orderBy(F.col("penalties").desc()).limit(10))


# ---------------- KPI 3: BLOCKS VS SOG ----------------

# Team-level only for now.
# fact_shot_attempt does not carry blocker identity, so player-level blocker attribution
# is not reliable from this table. Better to be explicit than fake precision here.
blocks_vs_sog_team = (
    shot
    .filter(F.col("event_owner_team_id").isNotNull())
    .groupBy(
        "season",
        F.lit("team").alias("entity_type"),
        F.col("event_owner_team_id").alias("entity_id"),
        "owner_strength_state",
    )
    .agg(
        F.sum(F.col("is_blocked")).alias("blocks"),
        F.sum(F.col("is_sog")).alias("sog"),
    )
)

overwrite_table(KPI_BLOCKS_VS_SOG, blocks_vs_sog_team)
print(f"Overwrote: {KPI_BLOCKS_VS_SOG} | rows = {spark.table(KPI_BLOCKS_VS_SOG).count()}")
display(spark.table(KPI_BLOCKS_VS_SOG).orderBy(F.col("blocks").desc()).limit(10))


# ---------------- KPI 4: PLAYER SHOT MISS MAP ----------------

# This is another spatial KPI where binning + rink folding matters.
# We keep attempts and misses alongside miss_rate so QA is easier downstream.
miss_map = (
    shot_binned
    .filter(F.col("shooter_player_id").isNotNull())
    .groupBy("season", F.col("shooter_player_id").alias("player_id"), "x_bin", "y_bin", "shot_type")
    .agg(
        F.count(F.lit(1)).alias("attempts"),
        F.sum(F.col("is_miss")).alias("misses"),
    )
    .withColumn(
        "miss_rate",
        F.when(F.col("attempts") > 0, F.col("misses") / F.col("attempts")).otherwise(F.lit(None)),
    )
    .select(
        "season",
        "player_id",
        "x_bin",
        "y_bin",
        "shot_type",
        "attempts",
        "misses",
        "miss_rate",
    )
    .transform(lambda df: add_rink_xy_from_bins(df, MAX_X_BIN, MAX_Y_BIN, CENTER_X_BIN))
    .transform(lambda df: add_viz_rink_xy(df, MAX_RINK_X, MAX_RINK_Y))
)

overwrite_table(KPI_MISS_MAP, miss_map)
print(f"Overwrote: {KPI_MISS_MAP} | rows = {spark.table(KPI_MISS_MAP).count()}")
display(spark.table(KPI_MISS_MAP).orderBy(F.col("attempts").desc()).limit(10))


# ---------------- KPI 5: HITS PER 60 BY STRENGTH STATE ----------------

# True hits/60 would use actual TOI as the denominator.
# Until TOI is added, this uses a reasonable proxy based on shot-event time spans
# within each team/game/state bucket. Not perfect, but directionally useful.
hit_counts = (
    hit
    .filter(F.col("event_owner_team_id").isNotNull())
    .groupBy("season", "game_id", F.col("event_owner_team_id").alias("team_id"), "owner_strength_state")
    .agg(F.count(F.lit(1)).alias("hits"))
)

shot_time_spans = (
    shot
    .filter(F.col("event_owner_team_id").isNotNull())
    .withColumn("sec_in_period", time_in_period_to_seconds(F.col("time_in_period")))
    .groupBy(
        "season",
        "game_id",
        F.col("event_owner_team_id").alias("team_id"),
        "owner_strength_state",
        "period_number",
    )
    .agg(
        F.min("sec_in_period").alias("min_sec"),
        F.max("sec_in_period").alias("max_sec"),
    )
    .withColumn("span_sec", (F.col("max_sec") - F.col("min_sec")).cast("int"))
    .groupBy("season", "game_id", "team_id", "owner_strength_state")
    .agg(F.sum("span_sec").alias("proxy_seconds"))
    .withColumn("proxy_minutes", F.col("proxy_seconds") / F.lit(60.0))
)

hits_60 = (
    hit_counts
    .join(shot_time_spans, on=["season", "game_id", "team_id", "owner_strength_state"], how="left")
    .withColumn(
        "hits_per_60",
        F.when(
            (F.col("proxy_minutes").isNotNull()) & (F.col("proxy_minutes") > 0),
            F.col("hits") / (F.col("proxy_minutes") / F.lit(60.0)),
        ).otherwise(F.lit(None)),
    )
    .groupBy("season", "team_id", "owner_strength_state")
    .agg(
        F.sum("hits").alias("hits"),
        F.sum("proxy_seconds").alias("proxy_seconds"),
        F.avg("hits_per_60").alias("hits_per_60"),
    )
)

overwrite_table(KPI_HITS_60, hits_60)
print(f"Overwrote: {KPI_HITS_60} | rows = {spark.table(KPI_HITS_60).count()}")
display(spark.table(KPI_HITS_60).orderBy(F.col("hits").desc()).limit(10))

print("Gold KPI build complete.")