# Data Model Overview

## Overview

This project uses a hybrid dimensional model designed to support descriptive and diagnostic hockey analytics. 

Core entities such as players and teams are materialized as dimension tables in the Gold layer, while event-level fact tables and KPI aggregates are built in Databricks. Additional lightweight semantic dimensions are defined in Power BI to keep the lakehouse model focused and flexible.

The goal is to balance:
- clean, reusable data structures in the lakehouse
- flexible reporting logic in the BI layer

---

## Modeling Approach

The model follows a Medallion Architecture, with the Gold layer serving as the primary analytics model.

- **Core dimensions** (player, team) are materialized in Gold
- **Fact tables** capture granular in-game events
- **KPI tables** provide pre-aggregated, analytics-ready outputs
- **Semantic dimensions** (e.g., situation, zone, score state) are partially handled in Power BI

This results in a hybrid design where:
- Databricks handles heavy transformations and standardization
- Power BI handles lightweight semantic shaping and reporting logic

---

## Core Tables

### Dimension Tables (Gold)

- `gold.dim_player`
- `gold.dim_team`
- `gold.player_current_team`

These tables provide stable descriptive context for filtering, grouping, and reporting.

---

### Fact Tables (Gold)

- `gold.fact_shot_attempt`
- `gold.fact_penalty`
- `gold.fact_hit`

These tables represent the core event-level data used for all downstream analytics.

---

### KPI / Aggregate Tables (Gold)

Examples include:

- `kpi_goalie_location_grid`
- `kpi_penalties_taken_drawn`
- `kpi_blocks_vs_sog`
- `kpi_player_shot_miss_map`
- `kpi_hits_60_by_state`

These tables are pre-aggregated for performance and designed to directly support dashboards and analysis.

---

### BI Views (Databricks)

- `gold_bi.v_*` views expose reporting-friendly versions of Gold tables

These include:
- readable labels (e.g., zone names)
- parsed strength state fields
- simplified schemas for BI consumption

---

## Data Grain

Understanding grain is critical for correct aggregation.

- `fact_shot_attempt`: one row per shot-related event  
- `fact_penalty`: one row per penalty event  
- `fact_hit`: one row per hit event  

- `kpi_goalie_location_grid`: one row per season, goalie, and location bin  
- `kpi_player_shot_miss_map`: one row per season, player, location bin, and shot type  
- `kpi_penalties_taken_drawn`: one row per entity, role, situation, and penalty attributes  
- `kpi_hits_60_by_state`: one row per team and strength state  

---

## Relationships

- Fact tables join to dimension tables via:
  - `player_id`
  - `team_id`
  - `game_id`

- KPI tables are derived from Gold fact tables and inherit their keys and structure

- BI layer relationships are partially defined in Power BI to support flexible reporting

---

## Key Design Decisions

### Curated Gold Schemas
Gold fact tables intentionally include only relevant columns instead of carrying forward all Silver data.  
This keeps the model clean, performant, and easier to understand.

---

### KPI Tables vs On-the-Fly Calculations
Many analytics (e.g., shot maps, goalie performance, hits per 60) are precomputed in Databricks instead of fully calculated in Power BI.

This:
- improves dashboard performance
- simplifies reporting logic
- ensures consistent metric definitions

---

### Situation and Strength Handling
Raw `situation_code` is preserved in Gold and interpreted into reporting-friendly fields (e.g., strength state) in KPI tables and BI views.

This avoids hardcoding assumptions too early while still enabling clear reporting.

---

### Hybrid BI Layer
Semantic modeling is intentionally split:

- **Databricks**:
  - heavy transformations
  - KPI logic
  - standardized outputs

- **Power BI**:
  - lightweight dimensions (e.g., situation, zone, score state)
  - measures and thresholds
  - reporting relationships

This keeps the data platform flexible without overcomplicating the lakehouse model.

---

## Consumption

Gold tables and BI views support:

- Power BI dashboards (primary reporting layer)
- ad hoc analysis in Databricks notebooks

The model is designed for:
- fast aggregation
- flexible filtering
- clear interpretation of hockey-specific metrics
