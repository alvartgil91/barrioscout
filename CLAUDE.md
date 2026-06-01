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
  Dataform (definitions/) — 13 SQLX models
         │ staging views → intermediate tables → mart table
         ▼
  BigQuery analytics layer (barrioscout_analytics)
    └── agg_neighborhood_scores  ← main output, 241 zones
         │
         ▼
  Streamlit Dashboard (dashboard/app.py)  ← Phase 4, complete
```

---

## BigQuery Schema Structure

| Dataset                  | Purpose                                                   |
|--------------------------|-----------------------------------------------------------|
| `barrioscout_raw`        | Raw ingested data, minimal transformation, append-only    |
| `barrioscout_clean`      | Defined in `config/settings.py` but **NOT an active Dataform layer**. Actual intermediate layer is `barrioscout_staging`. |
| `barrioscout_staging`    | Dataform staging views (deduped, typed, normalised)       |
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
streamlit run dashboard/app.py
```

## Running Tests

```bash
# Source connectivity validation (no pytest, no BigQuery needed)
python tests/test_sources.py
```

---

## Dataform Pipeline (Phase 3 — complete)

Run with: `dataform run --tags phase3`

Rebuild only the mart (fast, ~40 MiB): `dataform run --actions agg_neighborhood_scores`

Models live in `definitions/` (13 transforming models + 9 source declarations):

| Model | Type | Dataset | Description |
|-------|------|---------|-------------|
| `stg_idealista_listings` | view | barrioscout_staging | Deduped listings with price_per_m2 |
| `stg_catastro_buildings` | view | barrioscout_staging | Residential buildings with ST_GEOGPOINT |
| `stg_osm_pois` | view | barrioscout_staging | OSM POIs with INITCAP(city) normalisation |
| `stg_neighborhoods` | view | barrioscout_staging | Cleaned polygons, surrogate IDs, zone_type derived |
| `fct_listing_observations` | table | barrioscout_analytics | One row per (property_id × day), spatial join to neighborhoods |
| `int_listing_status` | table | barrioscout_analytics | Latest status per property from listing_status_checks |
| `int_listings_latest` | table | barrioscout_analytics | Latest snapshot + lifecycle stats per property |
| `int_neighborhood_pois` | table | barrioscout_analytics | POI counts by category per neighborhood |
| `int_neighborhood_buildings` | table | barrioscout_analytics | Catastro building age stats per neighborhood |
| `int_neighborhood_listings` | table | barrioscout_analytics | Median prices, counts, rental yield per neighborhood |
| `dim_districts` | table | barrioscout_analytics | District dimension (Madrid 21 + Granada 8) |
| `dim_neighborhoods` | table | barrioscout_analytics | Neighborhood dimension (241 zones) with geometry + metro_area |
| `agg_neighborhood_scores` | table | barrioscout_analytics | Final score card: 5 sub-scores + composite + raw metrics |

### Scoring methodology

5 sub-scores, each `PERCENT_RANK() OVER (PARTITION BY city ORDER BY metric)` → multiplied by 100 (0–100 scale, within-city only):

| Sub-score | Metric | Weight | Coverage |
|-----------|--------|--------|----------|
| `services_score` | Mean percentile rank of health/education/shopping/transport POI counts | 20% | 100% (all neighborhoods) |
| `building_quality_score` | `pct_post_2000` ascending rank | 15% | ~87% (Catastro INSPIRE coverage) |
| `price_score` | `median_sale_price_m2` descending rank (cheaper = higher) | 20% | neighborhoods with ≥3 sale listings |
| `yield_score` | `gross_rental_yield_pct` ascending rank | 25% | neighborhoods with ≥3 sale AND ≥3 rent |
| `market_dynamics_score` | Mean of supply density + pricedrop ratio + discount magnitude percentile ranks | 20% | neighborhoods with ≥5 total listings |

`composite_score`: weighted average of non-NULL sub-scores; weight of missing sub-scores redistributed proportionally.

`data_completeness`: COUNT(non-NULL sub-scores) / 5. `available_sub_scores`: integer 0–5.

**Known limitations:**
- `services_score` reflects POI counts (quantity), not quality or distance. Social housing areas with concentrated public services score high, which may not reflect desirability.
- `building_quality_score` uses `pct_post_2000` — rewards newness, not quality. Catastro INSPIRE undercounts historic buildings in old city centers (e.g. Albaycín shows only 34 buildings with 73.5% post-2000 despite being medieval).
- `price_score` is an affordability signal (cheaper = better). This inverts in undesirable neighborhoods. Dashboard should show raw prices alongside the score.
- `yield_score` and `market_dynamics_score` require listing volume — coverage grows as email digest data accumulates.
- INE `renta` is municipal-level only — no sub-city income differentiation is possible with current data.
- Completeness penalty applies: ×1.00 for 5/5 sub-scores, ×0.95 for 4/5, ×0.85 for 3/5, ×0.70 for 2/5, ×0.50 for 1/5. Dashboard should warn on zones with ≤2 sub-scores.

**Key facts:**
- 241 scored zones: 166 city neighborhoods (131 Madrid + 35 Granada) + 75 metro zones
  - zone_type values: `capital_neighborhood` | `metro_neighborhood` | `metro_municipality`
- `dim_neighborhoods` deduplicates two Granada neighborhoods that appeared with multiple district assignments in the source (Joaquina Eguaras: Beiro+Norte; San Matías-Realejo: duplicate row)
- `building_quality_score` guards with `pct_post_2000 IS NOT NULL`, not `building_quality_prank IS NOT NULL` — because `PERCENT_RANK()` never returns NULL (NULLS LAST would assign high scores to no-Catastro neighborhoods)

---

## BigQuery — Current State (updated May 2026)

> ⚠️ Row counts verified May 2026. Always run BQ queries to confirm before starting a new session.

### Raw layer (`barrioscout_raw`)

| Table | Rows | Status |
|-------|------|--------|
| `neighborhoods` | 272 | ✅ 197 city + 75 metro zones |
| `idealista_listings` | 6,247 | ✅ Apr–May 2026 (historical Nov 2025–Mar 2026 unrecoverable) |
| `catastro_buildings` | 73,568 | ✅ Restored May 2026 |
| `listing_status_checks` | 0 | ✅ Schema correct, data accumulating |
| `osm_pois` | MISSING | ⚠️ Overpass API outage — `int_neighborhood_pois` intact in analytics |
| `ine_renta`, `ine_ipv`, `mtr_transacciones`, `mtr_valor_tasado` | unchanged | ✅ |

### Analytics layer (`barrioscout_analytics`)

| Table | Rows | Notes |
|-------|------|-------|
| `dim_neighborhoods` | 241 | 166 capital + 75 metro (after dedup of 272 raw rows) |
| `agg_neighborhood_scores` | 241 | 0 nulls in composite_score; PARTITION BY city |
| `int_neighborhood_pois` | 350 | Intact from last full run before Overpass outage |
| `int_neighborhood_buildings` | 209 | ✅ |
| `fct_listing_observations` | ~1,025 | Apr–May 2026 only |

### Geocoding notes
- Cloud Function: `alert_city` extracted from email, Google Maps API `components` bias, bbox validation with retry
- 6 listings manually corrected (4 bad geocodes Madrid + 2 Gran Vía Motril)
- `geocode_level = "UNVERIFIED"` safety net for coordinates outside city bbox
- Two-pass spatial join in `fct_listing_observations`: ST_WITHIN exact + ST_DWITHIN 200m fallback

### DOM activation status
- `median_days_on_market` is present in `agg_neighborhood_scores`
- **NOT activated**: range only 1–9 days globally (Madrid max: 8 days; Granada: 6 days)
- Threshold to activate: 30-day range between neighborhoods in any city with ≥3 neighborhoods
- No SQLX changes made; `dom_prank` not yet added to `market_dynamics_score`

---

## Known Issues (May 2026)

- **osm_pois missing**: Overpass API outage (last attempt: HTTP 406). Re-run `python -m src.ingestion.osm_pois` when recovered. Analytics layer (`int_neighborhood_pois`) is intact from the last full run.
- **Historical listings unrecoverable**: Nov 2025–Mar 2026 listings lost when table was truncated to fix schema mismatch. Only Apr–May 2026 data is present.
- **defaultTableExpirationMs**: A 60-day TTL was set on `barrioscout_raw` — now removed. Never set table expiration on raw datasets.
- **ALLOW_FIELD_ADDITION**: Added to `bq_loader.py` after May 2026 incident where Cloud Function added new columns and WRITE_APPEND jobs failed against the old schema.
- **241 zones scored vs 350 expected**: `neighborhoods` table has 272 rows; some Tier 1–4 metro municipality subdivisions are incomplete (see `scripts/subdivision_strategy.md`).
- **DOM not active**: Range too narrow (<30 days between neighborhoods). `dom_prank` not yet added to `market_dynamics_score`.
- **~420 orphan listings**: Listings that can't be assigned to any polygon (smaller metro municipalities below subdivision threshold).

---

## Phase Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Raw ingestion scripts (Idealista, OSM, Catastro, INE, Ministerio) | ✅ Complete |
| 2 | Neighborhood + district polygons (Madrid + Granada) | ✅ Complete |
| 2.7 | Pre-scoring fixes (mojibake, bus_stop POIs) | ✅ Complete |
| 2.8 | Geocoding fixes + metro polygons + spatial join improvement | ✅ Complete |
| 3 | Scoring engine (Dataform, `agg_neighborhood_scores`) | ✅ Complete |
| 4 | Streamlit dashboard | ✅ Complete (deployment files ready) |

---

## Cost Target

**0 €/month** — all GCP usage within free tier limits:
- BigQuery: < 10 GB storage, < 1 TB queries/month
- Cloud Function (europe-west1) + Cloud Scheduler every 6h — **PAUSED** (museum mode since May 2026)
- Dataform scheduled runs (2×/day) — **PAUSED**
- To reactivate: re-enable schedules in Cloud Scheduler console and Dataform console
