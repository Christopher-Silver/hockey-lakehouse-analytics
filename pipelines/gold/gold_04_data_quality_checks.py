# ============================================================
# gold_04_data_quality_checks.py
#
# Runs basic data quality checks against the Gold layer and
# writes the results to persistent DQ tables.
#
# Output tables:
#   - dbw_hockey_lakehouse.gold.dq_results
#   - dbw_hockey_lakehouse.gold.dq_failures
#
# What this script is doing:
#   - checks Gold fact row counts against filtered silver.events
#   - validates primary-key uniqueness
#   - checks critical fields for nulls
#   - runs a few domain / format / bounds sanity checks
#   - confirms KPI tables exist and are not empty
#   - stores failing sample rows for easier debugging
#
# Write pattern:
#   - append DQ results by run timestamp
#   - idempotent from a pipeline perspective, but each run creates
#     a new audit trail entry rather than replacing prior results
#
# Notes:
#   - this is intentionally a practical DQ layer, not a giant framework
#   - the goal is to catch bad upstream data early and leave a trail
#     when something starts drifting
#   - sample rows are stored as plain strings on purpose so this works
#     cleanly across runtimes without relying on JSON conversion
# ============================================================

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------- CONFIG ----------------

SILVER_EVENTS = "dbw_hockey_lakehouse.silver.events"

FACT_SHOT = "dbw_hockey_lakehouse.gold.fact_shot_attempt"
FACT_PEN = "dbw_hockey_lakehouse.gold.fact_penalty"
FACT_HIT = "dbw_hockey_lakehouse.gold.fact_hit"

KPI_GOALIE_GRID = "dbw_hockey_lakehouse.gold.kpi_goalie_location_grid"
KPI_PEN_TAKEDRAW = "dbw_hockey_lakehouse.gold.kpi_penalties_taken_drawn"
KPI_BLOCKS_VS_SOG = "dbw_hockey_lakehouse.gold.kpi_blocks_vs_sog"
KPI_MISS_MAP = "dbw_hockey_lakehouse.gold.kpi_player_shot_miss_map"
KPI_HITS_60 = "dbw_hockey_lakehouse.gold.kpi_hits_60_by_state"

DQ_RESULTS = "dbw_hockey_lakehouse.gold.dq_results"
DQ_FAILURES = "dbw_hockey_lakehouse.gold.dq_failures"

SHOT_EVENT_TYPES = ["shot-on-goal", "missed-shot", "blocked-shot", "goal"]
PEN_EVENT_TYPES = ["penalty"]
HIT_EVENT_TYPES = ["hit"]

VALID_ZONE_CODES = ["O", "D", "N"]

# Loose coordinate bounds.
# These are a basic sanity check, not a strict rink-modeling rule.
X_MIN, X_MAX = -110.0, 110.0
Y_MIN, Y_MAX = -60.0, 60.0

FAIL_SAMPLE_N = 50


# ---------------- DQ TABLE SCHEMAS ----------------

dq_results_schema = T.StructType([
    T.StructField("run_ts", T.TimestampType(), False),
    T.StructField("layer", T.StringType(), False),
    T.StructField("table_name", T.StringType(), False),
    T.StructField("test_name", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("fail_count", T.LongType(), False),
    T.StructField("total_count", T.LongType(), True),
    T.StructField("fail_rate", T.DoubleType(), True),
    T.StructField("notes", T.StringType(), True),
])

# Keep failure samples simple and readable.
# Key fields help when tracing back to the original event.
dq_failures_schema = T.StructType([
    T.StructField("run_ts", T.TimestampType(), False),
    T.StructField("table_name", T.StringType(), False),
    T.StructField("test_name", T.StringType(), False),
    T.StructField("season", T.StringType(), True),
    T.StructField("game_id", T.StringType(), True),
    T.StructField("event_id", T.StringType(), True),
    T.StructField("sample_str", T.StringType(), False),
])


# ---------------- SETUP ----------------

def ensure_table(table_name: str, schema: T.StructType) -> None:
    if not spark.catalog.tableExists(table_name):
        (
            spark.createDataFrame([], schema)
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )


ensure_table(DQ_RESULTS, dq_results_schema)
ensure_table(DQ_FAILURES, dq_failures_schema)

run_ts = datetime.now(timezone.utc)

results_rows = []
failures_rows = []


# ---------------- RESULT HELPERS ----------------

def add_result(
    layer: str,
    table_name: str,
    test_name: str,
    fail_count: int,
    total_count: int | None = None,
    status: str | None = None,
    notes: str | None = None,
) -> None:
    if total_count is not None and total_count > 0:
        fail_rate = float(fail_count) / float(total_count)
    else:
        fail_rate = None

    if status is None:
        status = "PASS" if fail_count == 0 else "FAIL"

    results_rows.append(
        (
            run_ts,
            layer,
            table_name,
            test_name,
            status,
            int(fail_count),
            int(total_count) if total_count is not None else None,
            float(fail_rate) if fail_rate is not None else None,
            notes,
        )
    )


def add_fail_samples(table_name: str, test_name: str, df_bad: DataFrame) -> None:
    """
    Stores up to FAIL_SAMPLE_N failing rows as plain strings.

    This is mostly for debugging later. When a DQ check flips from PASS to WARN/FAIL,
    having a few real bad rows saved makes life much easier.
    """
    if not df_bad.columns:
        return

    has_season = "season" in df_bad.columns
    has_game = "game_id" in df_bad.columns
    has_event = "event_id" in df_bad.columns

    all_cols = [F.col(c) for c in df_bad.columns]

    sample_df = (
        df_bad
        .select(
            (F.col("season").cast("string") if has_season else F.lit(None)).alias("season"),
            (F.col("game_id").cast("string") if has_game else F.lit(None)).alias("game_id"),
            (F.col("event_id").cast("string") if has_event else F.lit(None)).alias("event_id"),
            F.struct(*all_cols).cast("string").alias("sample_str"),
        )
        .limit(FAIL_SAMPLE_N)
    )

    for row in sample_df.collect():
        failures_rows.append(
            (
                run_ts,
                table_name,
                test_name,
                row["season"],
                row["game_id"],
                row["event_id"],
                row["sample_str"],
            )
        )


# ---------------- LOAD TABLES ----------------

silver = spark.table(SILVER_EVENTS).withColumn("event_type_lc", F.lower(F.col("event_type")))
shot = spark.table(FACT_SHOT)
pen = spark.table(FACT_PEN)
hit = spark.table(FACT_HIT)

kpi_tables = [
    KPI_GOALIE_GRID,
    KPI_PEN_TAKEDRAW,
    KPI_BLOCKS_VS_SOG,
    KPI_MISS_MAP,
    KPI_HITS_60,
]


# ---------------- 1) ROW COUNT SANITY ----------------

# Basic sanity check:
# Gold fact counts should line up with the expected filtered slices from silver.events.
# This helps catch missing filters, accidental duplication, or partial rebuild issues.

silver_shot_cnt = silver.filter(F.col("event_type_lc").isin([e.lower() for e in SHOT_EVENT_TYPES])).count()
gold_shot_cnt = shot.count()
add_result(
    "gold_fact",
    FACT_SHOT,
    "rowcount_matches_silver_filtered",
    abs(gold_shot_cnt - silver_shot_cnt),
    total_count=silver_shot_cnt,
    status="PASS" if gold_shot_cnt == silver_shot_cnt else "WARN",
    notes=f"silver_filtered={silver_shot_cnt}, gold={gold_shot_cnt}",
)

silver_pen_cnt = silver.filter(F.col("event_type_lc").isin([e.lower() for e in PEN_EVENT_TYPES])).count()
gold_pen_cnt = pen.count()
add_result(
    "gold_fact",
    FACT_PEN,
    "rowcount_matches_silver_filtered",
    abs(gold_pen_cnt - silver_pen_cnt),
    total_count=silver_pen_cnt,
    status="PASS" if gold_pen_cnt == silver_pen_cnt else "WARN",
    notes=f"silver_filtered={silver_pen_cnt}, gold={gold_pen_cnt}",
)

silver_hit_cnt = silver.filter(F.col("event_type_lc").isin([e.lower() for e in HIT_EVENT_TYPES])).count()
gold_hit_cnt = hit.count()
add_result(
    "gold_fact",
    FACT_HIT,
    "rowcount_matches_silver_filtered",
    abs(gold_hit_cnt - silver_hit_cnt),
    total_count=silver_hit_cnt,
    status="PASS" if gold_hit_cnt == silver_hit_cnt else "WARN",
    notes=f"silver_filtered={silver_hit_cnt}, gold={gold_hit_cnt}",
)


# ---------------- 2) PK UNIQUENESS ----------------

def dq_pk_uniqueness(df: DataFrame, table_name: str, key_cols: list[str]) -> None:
    """
    Checks that the expected event grain is unique.

    Why this matters:
    duplicate event keys are one of the fastest ways to corrupt downstream KPIs,
    especially when aggregations start double-counting shots, penalties, or hits.
    """
    total = df.count()

    dup_df = (
        df.groupBy(*key_cols)
        .count()
        .filter(F.col("count") > 1)
    )
    dup_groups = dup_df.count()

    add_result(
        "gold_fact",
        table_name,
        f"pk_unique_{'_'.join(key_cols)}",
        dup_groups,
        total_count=total,
        status="PASS" if dup_groups == 0 else "FAIL",
        notes="fail_count = duplicated key groups",
    )

    if dup_groups > 0:
        bad = df.join(dup_df.select(*key_cols), on=key_cols, how="inner")
        add_fail_samples(table_name, f"pk_unique_{'_'.join(key_cols)}", bad)


dq_pk_uniqueness(shot, FACT_SHOT, ["season", "game_id", "event_id"])
dq_pk_uniqueness(pen, FACT_PEN, ["season", "game_id", "event_id"])
dq_pk_uniqueness(hit, FACT_HIT, ["season", "game_id", "event_id"])


# ---------------- 3) NULL CHECKS ----------------

def dq_null_check(df: DataFrame, table_name: str, col_name: str, layer: str = "gold_fact") -> None:
    """
    Checks for nulls in important fields.

    These are mostly critical ID / context columns. A few nulls here can quietly
    break joins, groupings, or dashboard filters downstream.
    """
    total = df.count()
    bad = df.filter(F.col(col_name).isNull())
    fail = bad.count()

    add_result(
        layer,
        table_name,
        f"null_check_{col_name}",
        fail,
        total_count=total,
        status="PASS" if fail == 0 else "WARN",
    )

    if fail > 0:
        add_fail_samples(table_name, f"null_check_{col_name}", bad)


for col_name in ["season", "game_id", "event_id", "event_type", "event_owner_team_id", "situation_code"]:
    if col_name in shot.columns:
        dq_null_check(shot, FACT_SHOT, col_name)
    if col_name in pen.columns:
        dq_null_check(pen, FACT_PEN, col_name)
    if col_name in hit.columns:
        dq_null_check(hit, FACT_HIT, col_name)

# A few role / metric fields matter enough to check explicitly too.
if "shooter_player_id" in shot.columns:
    dq_null_check(shot, FACT_SHOT, "shooter_player_id")

if "goalie_player_id" in shot.columns:
    dq_null_check(shot, FACT_SHOT, "goalie_player_id")

if "penalized_player_id" in pen.columns:
    dq_null_check(pen, FACT_PEN, "penalized_player_id")

if "penalty_minutes" in pen.columns:
    dq_null_check(pen, FACT_PEN, "penalty_minutes")

if "hitter_player_id" in hit.columns:
    dq_null_check(hit, FACT_HIT, "hitter_player_id")


# ---------------- 4) DOMAIN / FORMAT / BOUNDS CHECKS ----------------

def dq_domain(
    df: DataFrame,
    table_name: str,
    col_name: str,
    valid_values: list,
    allow_null: bool = True,
) -> None:
    """
    Checks whether coded fields stay inside the expected domain.

    This helps catch bad upstream mapping, malformed source values, or unexpected
    new codes before they leak into reports.
    """
    total = df.count()

    if allow_null:
        bad = df.filter(F.col(col_name).isNotNull() & (~F.col(col_name).isin(valid_values)))
    else:
        bad = df.filter(~F.col(col_name).isin(valid_values))

    fail = bad.count()

    add_result(
        "gold_fact",
        table_name,
        f"domain_{col_name}",
        fail,
        total_count=total,
        status="PASS" if fail == 0 else "WARN",
        notes=f"valid={valid_values}",
    )

    if fail > 0:
        add_fail_samples(table_name, f"domain_{col_name}", bad)


for table_name, df in [(FACT_SHOT, shot), (FACT_PEN, pen), (FACT_HIT, hit)]:
    if "zone_code" in df.columns:
        dq_domain(df, table_name, "zone_code", VALID_ZONE_CODES, allow_null=True)


def dq_situation_code(df: DataFrame, table_name: str) -> None:
    """
    Checks that situation_code is four digits after padding.

    This is important because a lot of downstream strength-state logic depends on
    reliable parsing of that field. If the format drifts, those KPIs get messy fast.
    """
    total = df.count()
    sc = F.lpad(F.col("situation_code").cast("string"), 4, "0")

    bad = df.filter(~sc.rlike("^[0-9]{4}$"))
    fail = bad.count()

    add_result(
        "gold_fact",
        table_name,
        "format_situation_code_4digits",
        fail,
        total_count=total,
        status="PASS" if fail == 0 else "WARN",
    )

    if fail > 0:
        add_fail_samples(table_name, "format_situation_code_4digits", bad)


dq_situation_code(shot, FACT_SHOT)
dq_situation_code(pen, FACT_PEN)
dq_situation_code(hit, FACT_HIT)


if "penalty_minutes" in pen.columns:
    total = pen.count()

    # Another basic sanity check. Penalty minutes should exist and be positive.
    bad = pen.filter((F.col("penalty_minutes").isNull()) | (F.col("penalty_minutes") <= 0))
    fail = bad.count()

    add_result(
        "gold_fact",
        FACT_PEN,
        "penalty_minutes_positive",
        fail,
        total_count=total,
        status="PASS" if fail == 0 else "WARN",
    )

    if fail > 0:
        add_fail_samples(FACT_PEN, "penalty_minutes_positive", bad)


def dq_xy_bounds(df: DataFrame, table_name: str) -> None:
    """
    Checks that x/y values stay inside a loose expected range.

    This will not catch every weird spatial issue, but it helps catch clearly bad
    upstream coordinates before they distort map-based KPIs.
    """
    total = df.count()

    bad = df.filter(
        (F.col("x").isNotNull() & ((F.col("x") < F.lit(X_MIN)) | (F.col("x") > F.lit(X_MAX))))
        | (F.col("y").isNotNull() & ((F.col("y") < F.lit(Y_MIN)) | (F.col("y") > F.lit(Y_MAX))))
    )
    fail = bad.count()

    add_result(
        "gold_fact",
        table_name,
        "xy_bounds_check",
        fail,
        total_count=total,
        status="PASS" if fail == 0 else "WARN",
        notes=f"x=[{X_MIN},{X_MAX}] y=[{Y_MIN},{Y_MAX}]",
    )

    if fail > 0:
        add_fail_samples(table_name, "xy_bounds_check", bad)


dq_xy_bounds(shot, FACT_SHOT)
dq_xy_bounds(pen, FACT_PEN)
dq_xy_bounds(hit, FACT_HIT)


# ---------------- 5) FRESHNESS / INGESTION CHECK ----------------

def dq_ingested_at(df: DataFrame, table_name: str) -> None:
    """
    Checks that ingested_at exists and is populated.

    This helps with traceability. Even if the data itself looks fine, missing
    ingestion timestamps make debugging and freshness monitoring a pain later on.
    """
    if "ingested_at" not in df.columns:
        add_result(
            "gold_fact",
            table_name,
            "ingested_at_present",
            1,
            total_count=None,
            status="WARN",
            notes="column missing",
        )
        return

    total = df.count()
    bad = df.filter(F.col("ingested_at").isNull())
    fail = bad.count()

    max_ts = df.agg(F.max("ingested_at").alias("max_ingested_at")).collect()[0]["max_ingested_at"]

    add_result(
        "gold_fact",
        table_name,
        "ingested_at_not_null",
        fail,
        total_count=total,
        status="PASS" if fail == 0 else "WARN",
        notes=f"max_ingested_at={max_ts}",
    )

    if fail > 0:
        add_fail_samples(table_name, "ingested_at_not_null", bad)


dq_ingested_at(shot, FACT_SHOT)
dq_ingested_at(pen, FACT_PEN)
dq_ingested_at(hit, FACT_HIT)


# ---------------- 6) KPI EXISTENCE / NON-EMPTY CHECKS ----------------

# These are lightweight checks, but still worth having.
# If a KPI table disappears or comes back empty, dashboards usually fail in less obvious ways.
for table_name in kpi_tables:
    exists = spark.catalog.tableExists(table_name)

    if not exists:
        add_result(
            "gold_kpi",
            table_name,
            "table_exists",
            1,
            total_count=None,
            status="FAIL",
            notes="missing KPI table",
        )
        continue

    row_count = spark.table(table_name).count()

    add_result(
        "gold_kpi",
        table_name,
        "non_empty",
        0 if row_count > 0 else 1,
        total_count=row_count,
        status="PASS" if row_count > 0 else "WARN",
        notes=f"rows={row_count}",
    )


# ---------------- WRITE RESULTS ----------------

results_df = spark.createDataFrame(results_rows, schema=dq_results_schema)
failures_df = spark.createDataFrame(failures_rows, schema=dq_failures_schema)

results_df.write.format("delta").mode("append").saveAsTable(DQ_RESULTS)
failures_df.write.format("delta").mode("append").saveAsTable(DQ_FAILURES)

print("DQ run written:")
print(" -", DQ_RESULTS)
print(" -", DQ_FAILURES)

display(
    spark.table(DQ_RESULTS)
    .filter(F.col("run_ts") == F.lit(run_ts))
    .orderBy(F.col("status").desc(), F.col("fail_count").desc())
)

display(
    spark.table(DQ_FAILURES)
    .filter(F.col("run_ts") == F.lit(run_ts))
    .limit(50)
)