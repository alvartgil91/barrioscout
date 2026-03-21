# BarrioScout — CLAUDE.md

## Project Purpose & Vision

BarrioScout is a real estate intelligence tool focused on the Spanish market (Granada and Madrid).
It collects data from public sources, calculates location and price scores by neighbourhood,
and presents investment opportunities through a Streamlit dashboard.

This is a professional portfolio project built by Alvaro (Senior Data Analyst / BI Engineer)
to demonstrate end-to-end data engineering skills on GCP.

---

## Architecture

```
Public Sources
  ├── Ministerio de Transportes  (quarterly transactions CSV)
  ├── Catastro INSPIRE           (property characteristics REST/XML)
  ├── OpenStreetMap Overpass     (POIs: schools, hospitals, supermarkets, metro, bus stops)
  ├── Google Places API          (service ratings & reviews)
  └── INE                        (socioeconomic data: median income, population)
         │
         ▼
  Python ETL scripts (src/ingestion/)
         │
         ▼
  BigQuery raw layer  (barrioscout_raw)
         │
         ▼
  Dataform (dataform/) — 9 SQLX models
         │ staging views → intermediate tables → mart table
         ▼
  BigQuery analytics layer (barrioscout_analytics)
    └── agg_neighborhood_scores  ← main output, 166 rows
         │
         ▼
  Streamlit Dashboard (src/app/)  ← Phase 4, not yet built
```

---

## BigQuery Schema Structure

| Dataset                  | Purpose                                                   |
|--------------------------|-----------------------------------------------------------|
| `barrioscout_raw`        | Raw ingested data, minimal transformation, append-only    |
| `barrioscout_clean`      | Deduplicated, typed, validated data                       |
| `barrioscout_analytics`  | Aggregated scores, rankings, neighbourhood summaries      |

GCP Project: `portfolio-alvartgil91`

---

## Cities & Coordinates

| City    | Latitude  | Longitude  | Bounding Box (S, W, N, E)                  |
|---------|-----------|------------|---------------------------------------------|
| Granada | 37.1773   | -3.5986    | 37.1200, -3.6500, 37.2300, -3.5400          |
| Madrid  | 40.4168   | -3.7038    | 40.3100, -3.8300, 40.5600, -3.5200          |

---

## Code Conventions

- **Language**: All code, docstrings, comments, variable names, and commit messages in **English**
- **Type hints**: Required on all functions
- **Import order**: stdlib → third-party → local
- **Ingestion pattern**: Every ingestion script must expose three functions:
  - `extract() -> <raw_type>` — fetch data from source
  - `transform(raw) -> pd.DataFrame` — clean and normalise
  - `load(df: pd.DataFrame) -> None` — write to BigQuery raw layer

---

## Running the Project

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your API keys and GCP credentials

# 3. Validate all data sources (no BigQuery required)
python tests/test_sources.py

# 4. Run ingestion (example)
python -m src.ingestion.ministerio

# 5. Launch dashboard
streamlit run src/app/main.py
```

## Running Tests

```bash
# Source connectivity validation (no pytest, no BigQuery needed)
python tests/test_sources.py
```

---

## Dataform Pipeline (Phase 3 — complete)

Run with: `cd dataform && dataform run --tags phase3`

Rebuild only the mart (fast, ~40 MiB): `dataform run --actions agg_neighborhood_scores`

| Model | Type | Dataset | Description |
|-------|------|---------|-------------|
| `stg_idealista_listings` | view | barrioscout_staging | Deduped listings with price_per_m2 |
| `stg_catastro_buildings` | view | barrioscout_staging | Residential buildings with ST_GEOGPOINT |
| `stg_osm_pois` | view | barrioscout_staging | OSM POIs with INITCAP(city) normalisation |
| `fct_listing_observations` | table | barrioscout_analytics | One row per (property_id × day), spatial join to neighborhoods |
| `int_listings_latest` | table | barrioscout_analytics | Latest snapshot + lifecycle stats per property |
| `int_neighborhood_pois` | table | barrioscout_analytics | POI counts by category per neighborhood |
| `int_neighborhood_buildings` | table | barrioscout_analytics | Catastro building age stats per neighborhood |
| `int_neighborhood_listings` | table | barrioscout_analytics | Median prices, counts, rental yield per neighborhood |
| `agg_neighborhood_scores` | table | barrioscout_analytics | Final score card: 5 sub-scores + composite + raw metrics |

### Scoring methodology

5 sub-scores, each `PERCENT_RANK() OVER (PARTITION BY city ORDER BY metric)` → multiplied by 100 (0–100 scale, within-city only):

| Sub-score | Metric | Weight | Coverage |
|-----------|--------|--------|----------|
| `walkability_score` | Mean percentile rank of health/education/shopping/transport POI counts | 30% | 100% (all neighborhoods) |
| `building_quality_score` | `pct_post_2000` ascending rank | 20% | ~87% (Catastro INSPIRE coverage) |
| `price_score` | `median_sale_price_m2` descending rank (cheaper = higher) | 20% | neighborhoods with ≥3 sale listings |
| `yield_score` | `gross_rental_yield_pct` ascending rank | 20% | neighborhoods with ≥3 sale AND ≥3 rent |
| `market_dynamics_score` | Mean of supply density + pricedrop ratio percentile ranks | 10% | neighborhoods with ≥5 total listings |

`composite_score`: weighted average of non-NULL sub-scores; weight of missing sub-scores redistributed proportionally.

`data_completeness`: COUNT(non-NULL sub-scores) / 5. `available_sub_scores`: integer 0–5.

**Known limitations:**
- Walkability reflects POI counts (quantity), not quality or distance. Social housing areas with concentrated public services score high, which may not reflect desirability.
- `building_quality_score` uses `pct_post_2000` — rewards newness, not quality. Catastro INSPIRE undercounts historic buildings in old city centers (e.g. Albaycín shows only 34 buildings with 73.5% post-2000 despite being medieval).
- `price_score` is an affordability signal (cheaper = better). This inverts in undesirable neighborhoods. Dashboard should show raw prices alongside the score.
- `yield_score` and `market_dynamics_score` require listing volume — coverage grows as email digest data accumulates.
- INE `renta` is municipal-level only — no sub-city income differentiation is possible with current data.
- Dashboard should **filter or label** neighborhoods with `data_completeness < 0.6` (fewer than 3/5 sub-scores) to avoid misleading rankings driven by 1–2 signals.

**Key facts:**
- 166 neighborhoods total: 131 Madrid + 35 Granada
- `dim_neighborhoods` deduplicates two Granada neighborhoods that appeared with multiple district assignments in the source (Joaquina Eguaras: Beiro+Norte; San Matías-Realejo: duplicate row)
- `building_quality_score` guards with `pct_post_2000 IS NOT NULL`, not `building_quality_prank IS NOT NULL` — because `PERCENT_RANK()` never returns NULL (NULLS LAST would assign high scores to no-Catastro neighborhoods)

---

## Phase Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Raw ingestion scripts (Idealista, OSM, Catastro, INE, Ministerio) | ✅ Complete |
| 2 | Neighborhood + district polygons (Madrid + Granada) | ✅ Complete |
| 2.7 | Pre-scoring fixes (mojibake, bus_stop POIs) | ✅ Complete |
| 3 | Scoring engine (Dataform, `agg_neighborhood_scores`) | ✅ Complete |
| 4 | Streamlit dashboard | ⬜ Next |

---

## Cost Target

**0 €/month** — all GCP usage within free tier limits:
- BigQuery: < 10 GB storage, < 1 TB queries/month
- No Cloud Functions, no Scheduler (cron via local scripts)
