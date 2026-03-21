# BarrioScout — Phase 3 Status Report

Generated: 2026-03-21
Phase 3: Scoring Engine (Dataform)

---

## 1. Dataform Model Inventory

All 9 models are live. Run with: `cd dataform && dataform run --tags phase3`

| Model | Type | Dataset | Rows | Status |
|-------|------|---------|------|--------|
| `stg_idealista_listings` | VIEW | barrioscout_staging | (view, reads raw) | ✅ |
| `stg_catastro_buildings` | VIEW | barrioscout_staging | (view, reads raw) | ✅ |
| `stg_osm_pois` | VIEW | barrioscout_staging | (view, reads raw) | ✅ |
| `stg_neighborhoods` | VIEW | barrioscout_staging | (view, reads raw) | ✅ pre-existing |
| `fct_listing_observations` | TABLE | barrioscout_analytics | 1,433 | ✅ |
| `int_listings_latest` | TABLE | barrioscout_analytics | 1,429 | ✅ |
| `int_neighborhood_pois` | TABLE | barrioscout_analytics | 166 | ✅ |
| `int_neighborhood_buildings` | TABLE | barrioscout_analytics | 144 | ✅ |
| `int_neighborhood_listings` | TABLE | barrioscout_analytics | 138 | ✅ |
| `agg_neighborhood_scores` | TABLE | barrioscout_analytics | 166 | ✅ |

---

## 2. BigQuery State

### barrioscout_raw (source tables)

| Table | Rows | Size (MB) |
|-------|------|-----------|
| `catastro_buildings` | 72,684 | 4.36 |
| `idealista_listings` | 1,525 | 0.68 |
| `ine_ipv` | 608 | 0.04 |
| `ine_renta` | 3,120 | 0.16 |
| `ministerio_transacciones` | 176 | 0.01 |
| `ministerio_valor_tasado` | 168 | 0.01 |
| `neighborhoods` | 197 | 1.21 |
| `osm_pois` | 16,480 | 1.35 |

Notes:
- `idealista_listings`: 1,525 raw rows → 1,429 unique properties (96 duplicate observations for the same day)
- `catastro_buildings`: 72,684 total → 57,402 residential after `current_use = '1_residential'` filter
- `osm_pois`: grew from 8,060 → 16,480 after adding `highway=bus_stop` to Overpass query (Phase 2.7)

### barrioscout_analytics (Dataform output)

| Table | Rows | Size (MB) | Notes |
|-------|------|-----------|-------|
| `dim_districts` | 29 | 0.28 | 21 Madrid + 8 Granada |
| `dim_neighborhoods` | 166 | 0.48 | 131 Madrid + 35 Granada |
| `fct_listing_observations` | 1,433 | 0.22 | 1 row per property × day |
| `int_listings_latest` | 1,429 | 0.25 | 1 row per property (latest snapshot) |
| `int_neighborhood_pois` | 166 | 0.01 | All 166 neighborhoods, zeros for missing |
| `int_neighborhood_buildings` | 144 | 0.01 | 22 neighborhoods have no Catastro coverage |
| `int_neighborhood_listings` | 138 | 0.01 | 28 neighborhoods have no active listings |
| `agg_neighborhood_scores` | 166 | 0.04 | 1 row per neighborhood |

**Neighborhood count note**: `dim_neighborhoods` was updated in Phase 3 to deduplicate two Granada neighborhoods that had multiple district assignments in the source data (Joaquina Eguaras spanning Beiro+Norte districts; San Matías-Realejo true duplicate). Corrected total: 166 (was previously inflated to 172).

---

## 3. Scoring Coverage

### Sub-score availability per city

| Sub-score | Granada (35 total) | Madrid (131 total) | Notes |
|-----------|--------------------|--------------------|-------|
| `walkability_score` | 35 (100%) | 131 (100%) | Always present; COALESCE(count, 0) for zero-POI areas |
| `building_quality_score` | 31 (89%) | 113 (86%) | Requires Catastro INSPIRE coverage |
| `price_score` | 14 (40%) | 44 (34%) | Requires ≥3 sale listings |
| `yield_score` | 10 (29%) | 23 (18%) | Requires ≥3 sale AND ≥3 rent listings |
| `market_dynamics_score` | 13 (37%) | 45 (34%) | Requires ≥5 total listings |
| **All 5 sub-scores** | **8 (23%)** | **14 (11%)** | Full coverage; most reliable composite |

### Data completeness summary

| City | Avg. completeness | Avg. composite score |
|------|------------------|---------------------|
| Granada | 0.589 | 38.7 |
| Madrid | 0.544 | 41.8 |

Completeness distribution (Granada):
- `available_sub_scores = 5`: 8 neighborhoods (23%)
- `available_sub_scores = 4`: included in the 61% with ≥3
- `available_sub_scores = 2`: majority (~50% of total)
- `available_sub_scores = 1`: 2 neighborhoods (walkability only)

---

## 4. Top/Bottom 5 Neighborhoods by Composite Score

### Granada — Top 5

| Rank | Neighborhood | Composite | Walk | Bldg | Price | Yield | Mkt | Subs |
|------|-------------|-----------|------|------|-------|-------|-----|------|
| 1 | Albaycín | 71.8 | 64.7 | 82.4 | NULL | NULL | NULL | 2 |
| 2 | Campus de la Salud | 69.2 | 56.6 | 88.2 | NULL | NULL | NULL | 2 |
| 3 | Almanjáyar | 68.4 | 72.8 | 61.8 | NULL | NULL | NULL | 2 |
| 4 | Carretera de la Sierra | 60.9 | 48.5 | 79.4 | NULL | NULL | NULL | 2 |
| 5 | Castaño-Mirasierra | 56.2 | 54.4 | 58.8 | NULL | NULL | NULL | 2 |

**Best fully-scored Granada neighborhood**: Zaidín (composite 54.4, all 5 sub-scores, 88 POIs/km², 648 buildings)

### Granada — Bottom 5

| Rank | Neighborhood | Composite | Subs |
|------|-------------|-----------|------|
| 131 | Rey Badis | 12.1 | 2 |
| 130 | Campo Verde | 14.1 | 2 |
| 129 | Casería de Montijo | 18.0 | 2 |
| 128 | San Pedro | 18.4 | 1 |
| 127 | Villa Argaz | 19.3 | 2 |

### Madrid — Top 5

| Rank | Neighborhood | Composite | Walk | Bldg | Price | Yield | Mkt | Subs |
|------|-------------|-----------|------|------|-------|-------|-----|------|
| 1 | Buenavista | 82.5 | 91.9 | 68.5 | NULL | NULL | NULL | 2 |
| 2 | Prosperidad | 78.7 | 78.7 | NULL | NULL | NULL | NULL | 1 |
| 3 | Valdefuentes | 76.4 | 95.6 | 80.8 | NULL | NULL | 10.0 | 3 |
| 4 | Mirasierra | 75.6 | 76.3 | 74.6 | NULL | NULL | NULL | 2 |
| 5 | Canillas | 71.8 | 81.7 | 56.9 | NULL | NULL | NULL | 2 |

**Best fully-scored Madrid neighborhood**: Acacias (composite 67.9, all 5 sub-scores — to be verified)

### Madrid — Bottom 5

| Rank | Neighborhood | Composite | Subs |
|------|-------------|-----------|------|
| 1 | Amposta | 4.3 | 2 |
| 2 | Hellín | 4.4 | 2 |
| 3 | El Pardo | 9.4 | 2 |
| 4 | Vinateros | 10.6 | 2 |
| 5 | San Cristóbal | 11.7 | 2 |

---

## 5. Known Issues

### 5.1 Almanjáyar Anomaly — Root Cause and Recommendation

**Observation**: Almanjáyar ranks #3 in Granada (composite 68.4), above Centro-Sagrario and Zaidín. Almanjáyar is one of Granada's most economically disadvantaged neighborhoods (historically high unemployment, social housing estates).

**Raw metrics for Almanjáyar**:
- `residential_buildings`: 104 | `avg_year_built`: 1990 | `median_year_built`: 1983 | `pct_post_2000`: 23.1% | `pct_pre_1960`: 0.0%
- `health_count`: 7 | `education_count`: 8 | `shopping_count`: 2 | `transport_count`: 34 | `total_pois`: 51
- `sale_count`: 2 | `rent_count`: 2 | `total_listings`: 4
- `available_sub_scores`: 2 (only `walkability_score` and `building_quality_score`)

**Root causes** (in order of impact):

1. **Data completeness gap (primary cause)**: Almanjáyar has only 4 total listings, all below the thresholds for `price_score` (need ≥3 sale), `yield_score` (need ≥3 sale AND ≥3 rent), and `market_dynamics_score` (need ≥5 total). The composite is computed from only 2/5 sub-scores. With walkability=72.8 and building_quality=61.8 both above-median, composite inflates to 68.4. The missing signals (price, yield, market activity) would almost certainly drag this down once listing data accumulates — Almanjáyar is not an active investment market.

2. **Walkability bias from concentrated social services**: Almanjáyar has 7 health facilities and 8 schools — social housing blocks in Spain are typically built near concentrated public services. It also has 34 bus stops (transit-dependent population). This inflates `walkability_score` (72.8) for the wrong reasons: service density reflects urban planning of public housing, not neighborhood desirability.

3. **Building quality misread**: `pct_post_2000=23.1%` with no pre-1960 buildings scores as "modern." But the 1980s social housing in Almanjáyar is functionally obsolete, not desirable. The `building_quality_score` (61.8) doesn't distinguish between quality new builds and deteriorating social housing stock.

**Comparison context**:
- Centro-Sagrario (city center, all amenities): composite 49.0 (5/5 sub-scores; price=14.7, yield=8.8 drag it down)
- Zaidín (densest Granada neighborhood, 648 buildings): composite 54.4 (5/5 sub-scores)
- Almanjáyar: 68.4 based on 2/5 signals

**Recommendations**:

1. **Dashboard gate (immediate, Phase 4)**: Do not show composite score (or mark as "Low confidence") for neighborhoods with `available_sub_scores < 3`. This would correctly exclude Almanjáyar, Buenavista, Prosperidad, and ~70% of neighborhoods from ranking until more listing data accumulates. Show these as "Insufficient data."

2. **Walkability signal refinement (future)**: Weight transport POIs lower than health/education/shopping, or use `pois_per_km2` (density) rather than raw counts. Almanjáyar's `pois_per_km2 = 35.1` is actually below the Granada median, which would reduce its walkability if density-adjusted.

3. **Building quality signal refinement (future)**: Replace or supplement `pct_post_2000` with a diversity metric (e.g., % non-residential use, floor-area ratio, mix of construction eras). This requires deeper Catastro attributes.

4. **Self-correcting with more data**: As listing volume grows, Almanjáyar's missing signals will be populated. Low prices (affordability score higher), likely low yields and low market dynamics will bring its composite to a more realistic level.

### 5.2 Bug Found and Fixed: building_quality_score NULL Guard

**Issue**: The original `agg_neighborhood_scores.sqlx` guarded `building_quality_score` with `WHEN building_quality_prank IS NOT NULL`. However, `PERCENT_RANK()` never returns NULL — it always produces a value. For neighborhoods with no Catastro data (`pct_post_2000 IS NULL`), the `NULLS LAST` ordering in the window function placed them at the top of the rank, assigning a score near 100.

**Effect**: Before the fix, neighborhoods with zero Catastro coverage (e.g., Centro-Sagrario, La Paz, San Pedro) received `building_quality_score ≈ 91.2` — the top percentile rank — despite having no building data. This was spurious inflation.

**Fix applied**: Changed guard to `WHEN pct_post_2000 IS NOT NULL`. Effect on coverage:
- Granada `has_bldg`: 35 → 31 (4 neighborhoods correctly now show NULL)
- Madrid `has_bldg`: 131 → 113 (18 neighborhoods correctly now show NULL)

### 5.3 Catastro INSPIRE Undercounts Historic Building Stock

Albaycín (medieval Moorish quarter, ~10th century) shows only 34 buildings in Catastro, with `pct_post_2000 = 73.5%` and `median_year_built = 2002`. This is clearly a data quality artifact — the INSPIRE endpoint records mainly registered constructions and renovations, and undercounts the historic building stock that was never formally re-registered.

**Effect**: Albaycín's `building_quality_score` (82.4) is inflated by a non-representative sample of its newest structures, not its real building stock. The historic center has 19th–early-20th century buildings that are entirely absent.

**Mitigation**: No easy fix without a better source. Dashboard should avoid presenting building_quality as the primary signal for historic-center neighborhoods. Flagging neighborhoods with `residential_buildings < 50` as "low Catastro coverage" in the dashboard would help.

### 5.4 Sparse Yield Coverage (Expected, Grows Over Time)

Only 10 Granada and 23 Madrid neighborhoods have `yield_score` populated. This requires ≥3 sale listings AND ≥3 rent listings in the same neighborhood. With 1,429 unique properties and only ~5 months of data collection, most neighborhoods have too few observations.

**Self-correcting**: As the Idealista email digest accumulates data, more neighborhoods will cross the threshold. No fix needed now.

### 5.5 Price Score Direction (Known Trade-off)

`price_score` is inverted (cheaper = higher score, designed as an "affordability" signal for investors). This can reward genuinely affordable neighborhoods but also low-quality ones. This is documented and acceptable at the MVP stage. Dashboard should always display `median_sale_price_m2` alongside `price_score`.

---

## 6. Phase Assessment

### Completed Phases

| Phase | Description | Outcome |
|-------|-------------|---------|
| Phase 1 | Ingestion pipeline (Idealista, OSM, Catastro, INE, Ministerio) | ✅ 8 raw tables, ~95K rows |
| Phase 2 | Neighborhood polygons (Madrid + Granada, 166 areas) | ✅ Spatial joins working |
| Phase 2.7 | Pre-scoring fixes | ✅ Mojibake fixed, bus_stop added (8,060→16,480 OSM POIs) |
| Phase 3 | Scoring engine (Dataform) | ✅ 166 rows in `agg_neighborhood_scores` |

### Current State

The scoring engine is functional. `agg_neighborhood_scores` produces composite scores for all 166 neighborhoods. Coverage is limited by available listing data (most neighborhoods have walkability + building_quality only; price/yield/market require volume).

A significant structural finding: **~75% of neighborhoods currently have fewer than 3/5 sub-scores**. The dashboard must handle this gracefully — ranking all 166 neighborhoods by composite would be misleading. The recommended approach is to present fully-scored neighborhoods prominently and show partial-data neighborhoods with a clear confidence indicator.

### Phase 4 — Streamlit Dashboard (Next)

Priority items for dashboard design informed by this analysis:
1. Filter/label neighborhoods with `data_completeness < 0.6` as "Low confidence"
2. Show `available_sub_scores` as a badge on each neighborhood card
3. Display raw metrics (`median_sale_price_m2`, `pois_per_km2`, `pct_post_2000`) alongside sub-scores
4. City-scoped rankings only (Madrid vs Granada are different markets)
5. Consider a "Data quality" view showing Catastro and listing coverage per neighborhood
