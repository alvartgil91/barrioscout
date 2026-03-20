# BarrioScout — Scoring Feasibility Analysis
*Generated: 2026-03-20 | Data window: 2026-03-16 → 2026-03-20 (5 days)*

---

## 1. Updated Idealista Profile

### 1.1 Schema (22 columns)

| Column | Type | Notes |
|---|---|---|
| property_id | STRING | |
| operation_type | STRING | |
| property_type | STRING | |
| address | STRING | |
| city | STRING | |
| price | FLOAT64 | |
| previous_price | FLOAT64 | priceDrop emails only |
| discount_pct | FLOAT64 | priceDrop emails only |
| area_m2 | FLOAT64 | |
| bedrooms | INT64 | |
| floor | INT64 | |
| is_exterior | BOOL | |
| has_elevator | BOOL | |
| property_url | STRING | **new vs old profile** |
| description | STRING | |
| image_url | STRING | |
| lat | FLOAT64 | |
| lon | FLOAT64 | |
| email_date | TIMESTAMP | |
| campaign_type | STRING | |
| email_id | STRING | |
| _loaded_at | TIMESTAMP | |

`property_url` is present and fully populated — added by parser fix iteration.

### 1.2 Null / Completeness (1,376 rows, 1,290 unique properties)

| Column | Nulls | Null % | vs old profile | Notes |
|---|---|---|---|---|
| property_id | 0 | 0% | — | |
| city | 0 | **0%** | ✅ fixed (was 16%+) | |
| address | 0 | **0%** | ✅ fixed | |
| lat | 0 | **0%** | ✅ fixed | |
| lon | 0 | **0%** | ✅ fixed | |
| price | 0 | 0% | — | |
| area_m2 | 3 | 0.2% | — | Near-perfect |
| operation_type | 0 | 0% | — | |
| property_url | 0 | 0% | **new column** | |
| description | 0 | 0% | — | |
| image_url | 0 | 0% | — | |
| bedrooms | 59 | 4.3% | — | fvp/luxury listings often omit |
| floor | 343 | 24.9% | — | Houses/villas have no floor concept |
| is_exterior | 1,356 | **98.5%** | expected | Intentional NULL when not stated (fix #4) |
| has_elevator | 1,359 | **98.7%** | expected | Intentional NULL when not stated (fix #5) |
| discount_pct | present in 319 rows | — | priceDrop only | |
| previous_price | present in 319 rows | — | priceDrop only | |

**Key finding:** The 16%+ null rate on city/address/lat/lon from the old 884-row profile is gone. All critical fields are 0% null. The high null rate on `is_exterior`/`has_elevator` is by design — the parser correctly sets NULL when the email doesn't state a value, rather than defaulting to False.

### 1.3 Volume by Operation Type and City (in-scope only)

| operation_type | City | Unique listings | Has area_m2 | Geocoded |
|---|---|---|---|---|
| sale | Madrid | 317 | 321 | 321 |
| sale | Granada | 190 | 246 | 247 |
| rent | Madrid | 231 | 240 | 240 |
| rent | Granada | 97 | 106 | 106 |
| **Total in-scope** | | **835** | | **914** |

462 listings (33.6%) are from out-of-scope municipalities (suburbs / metro area).

### 1.4 Price per m² Distribution (in-scope, deduped, area_m2 > 0 and < 1,000)

| operation_type | City | n | p25 | Median | p75 | Avg | Min | Max |
|---|---|---|---|---|---|---|---|---|
| rent | Granada | 97 | €9/m² | **€11/m²** | €13/m² | €11/m² | €7 | €21 |
| rent | Madrid | 231 | €18/m² | **€23/m²** | €29/m² | €25/m² | €10 | €200 |
| sale | Granada | 189 | €2,183/m² | **€2,727/m²** | €3,145/m² | €2,740/m² | €539 | €5,196 |
| sale | Madrid | 317 | €3,833/m² | **€5,292/m²** | €7,959/m² | €6,199/m² | €1,034 | €33,333 |

Sanity check vs Ministerio `ministerio_valor_tasado` historical avg: Granada €2,043/m², Madrid €3,616/m² — the Idealista median sale prices are ~33–46% higher, consistent with tasación (appraisal) values being conservative vs listing ask prices.

### 1.5 Known Data Issues in Idealista

- **86 within-batch duplicates** (6.7% of raw rows): Same property appearing twice within seconds — occurs when the same property_id is in both a `newAd` and a `priceDrop` email in the same 6h CF batch. All have `span_hours = 0`. Resolved by `ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY email_date DESC)` dedup in staging.
- **Encoding issue in `dim_neighborhoods`**: 31 Madrid neighborhood names are stored with corrupted UTF-8 (`Ã­` → `í`, `Ã±` → `ñ`, etc.). Root cause: the TopoJSON source uses UTF-8 but the BQ load treated it as latin-1. Affects display only — spatial geometry is correct. **Must fix before building the dashboard.**

---

## 2. Spatial Join Coverage

### 2.1 Overall Match Rate

| Scope | Total unique listings | Matched to neighborhood | Unmatched | Match % |
|---|---|---|---|---|
| All 1,290 listings | 1,290 | 820 | 470 | 63.6% |
| In-scope only (Granada + Madrid) | 835 | 820 | **15** | **98.2%** |
| Out-of-scope cities | 455 | 0 | 455 | 0% |

The 63.6% figure is misleading — virtually all unmatched listings are simply outside the scope (suburbs, satellite towns). For Granada and Madrid specifically, the spatial join is **98.2% effective**. The 15 unmatched in-scope listings are likely geocoding imprecisions that placed a point just outside a polygon boundary.

### 2.2 Neighborhood Coverage from Spatial Joins

| Source | Total neighborhoods | Matched ≥1 record | Unmatched (zero) |
|---|---|---|---|
| Listings → neighborhoods | 168 | **138** (82%) | 30 |
| POIs (education) → neighborhoods | 168 | 162 (96%) | 6 |
| POIs (health) → neighborhoods | 168 | 162 (96%) | 6 |
| POIs (shopping) → neighborhoods | 168 | 148 (88%) | 20 |
| POIs (transport) → neighborhoods | 168 | 126 (75%) | 42 |
| Catastro buildings → neighborhoods | 168 | **146** (87%) | 22 |

The 30 neighborhoods with zero listings are expected at 5 days of collection — the city periphery and lower-density barrios simply haven't appeared in alerts yet.

---

## 3. Per Sub-Score Feasibility

### 3.1 Sub-score 1: Rental Yield
*Median rent €/m² ÷ median sale €/m² per neighborhood*

**Data available:** 835 in-scope listings (321 sale Madrid, 247 sale Granada, 240 rent Madrid, 106 rent Granada), 98.2% geocoded.

| Metric | Granada | Madrid |
|---|---|---|
| Total neighborhoods | 37 | 131 |
| Neighborhoods with ≥1 listing (any) | 27 | 111 |
| **≥3 sale AND ≥3 rent** | **9 / 27 (33%)** | **22 / 111 (20%)** |
| **≥5 sale AND ≥5 rent** | **6 / 27 (22%)** | **6 / 111 (5.4%)** |
| Avg sale listings per neighborhood | 6.8 | 2.8 |
| Avg rent listings per neighborhood | 3.5 | 2.1 |
| Max sale in one neighborhood | 39 (Zaidín) | 30 (Sol) |
| Max rent in one neighborhood | 17 (San Matías) | 23 (Sol) |

**Top yield-ready neighborhoods (≥5 sale + ≥5 rent):**

| City | Neighborhood | Sale | Rent | Sale €/m² | Rent €/m² |
|---|---|---|---|---|---|
| Granada | San Matías-Realejo | 33 | 17 | €2,594 | €12.6/m² |
| Granada | Zaidín | 39 | 7 | €2,231 | €9.5/m² |
| Granada | Fígares | 24 | 15 | €3,011 | €12.4/m² |
| Granada | Centro-Sagrario | 7 | 12 | €3,325 | €13.0/m² |
| Granada | Pajaritos | 5 | 5 | €2,385 | €9.4/m² |
| Granada | Camino de Ronda | 9 | 6 | €3,083 | €12.1/m² |
| Madrid | Sol | 30 | 23 | €5,709 | €21.4/m² |
| Madrid | Universidad | 14 | 10 | €7,317 | €27.1/m² |
| Madrid | Trafalgar | 9 | 6 | €8,998 | €31.9/m² |
| Madrid | Goya | 7 | 7 | €10,321 | €29.4/m² |
| Madrid | Palacio | 6 | 6 | €8,175 | €27.1/m² |
| Madrid | Chopera | 6 | 4 | €6,706 | €23.6/m² |

**Verdict: ⚠️ Partial — viable for ~31 neighborhoods today, growing fast**

With only 5 days of data, 31/168 neighborhoods (18%) already have ≥3 of each type. Projecting linearly, at 30 days this would cover ~70% of neighborhoods. For MVP: compute yield where n≥3 for both types, suppress/flag where insufficient.

---

### 3.2 Sub-score 2: Affordability
*Listing price vs INE income per neighborhood*

**INE renta granularity:** 174 distinct municipalities for Granada province, 179 for Madrid. These are **municipality-level codes** — city names include Getafe, Alcobendas, Alcorcón, etc. Crucially:
- Madrid (28079): 1 single row per year — **city-level aggregate** (€19,632 net avg income in 2023)
- Granada (18087): 1 single row per year — **city-level aggregate**

There is **no sub-municipal (district or neighborhood) income data** from INE. The Atlas ADRH does publish section-level data but that's not what was ingested.

**Data available for affordability:**
- INE net average income per municipality (2015–2023, 9 years)
- Idealista listing prices per neighborhood (sale and rent)

**Verdict: ⚠️ Partial — city-level only, not neighborhood-level**

Can compute: "Price-to-income ratio at city level" (e.g. Madrid: median sale price €380k ÷ annual income €19.6k = 19.4 years). Cannot differentiate between neighborhoods based on income. This is a macro context metric, not a per-neighborhood affordability score.

**Upgrade path:** INE publishes Atlas de Distribución de Renta at census-section level (~2,000 sections in Madrid). This would enable neighborhood-level affordability — but requires a separate ingestion pipeline (not yet built).

---

### 3.3 Sub-score 3: Walkability / Services
*POI density per neighborhood by category*

| Category | Total POIs | Neighborhoods with ≥1 | Zero neighborhoods | Granada zeros | Madrid zeros |
|---|---|---|---|---|---|
| education | 2,672 | 162 / 168 (96%) | 6 | 6 | 0 |
| health | 3,038 | 162 / 168 (96%) | 6 | 5 | 1 |
| shopping | 1,300 | 148 / 168 (88%) | 20 | 13 | 7 |
| transport | 1,051 | 126 / 168 (75%) | 42 | **27** | 15 |

**Per-city averages:**

| Category | Granada avg | Madrid avg |
|---|---|---|
| education | 4.2 POIs/nbh | 14.3 POIs/nbh |
| health | 7.3 POIs/nbh | 18.5 POIs/nbh |
| shopping | 2.8 POIs/nbh | 7.0 POIs/nbh |
| transport | 0.8 POIs/nbh | 6.8 POIs/nbh |

**Notable issues:**
- **Granada transport is severely under-represented:** 27/37 neighborhoods (73%) have 0 transport POIs. This is likely a data gap — Granada has a limited metro/bus footprint and OSM coverage of bus stops may be incomplete. Do not include transport in the walkability score for Granada, or weight it separately.
- All Madrid neighborhoods have ≥1 education and health POI — excellent coverage.
- Shopping has some gaps in peripheral Madrid barrios (7 zeros) — expected.

**Verdict: ✅ Viable for education + health + shopping. ⚠️ Transport: viable for Madrid, not Granada**

---

### 3.4 Sub-score 4: Building Quality
*Average building age per neighborhood from Catastro*

| City | Neighborhoods with data | Total matched buildings | Avg per neighborhood | Avg year built | Range |
|---|---|---|---|---|---|
| Granada | 32 / 37 (86%) | 4,521 | 141 buildings | 1984 | 1957–2006 |
| Madrid | 114 / 131 (87%) | 36,038 | 316 buildings | 1975 | 1926–2021 |

- 99.8% of matched buildings have a valid `year_built` value
- 44% of buildings didn't match any neighborhood polygon (56% overall match rate). The Catastro centroids are derived from bbox midpoints, which can fall slightly outside polygon boundaries for buildings near the edges.
- 5 neighborhoods across both cities have zero buildings — these are likely parks, industrial zones, or areas at the edge of the polygon coverage.

**Verdict: ✅ Viable — strong coverage, 87% of neighborhoods have data**

---

### 3.5 Sub-score 5: Market Dynamics
*Price drop ratio + supply volume from Idealista; transaction volume from Ministerio*

**Ministerio data (city level only):**

| Dataset | Granada | Madrid | Coverage |
|---|---|---|---|
| Transactions (quarterly) | 88 records, 2004–2025 | 88 records, 2004–2025 | City-level municipality only |
| Appraised value €/m² | 84 records, 2005–2025 | 84 records, 2005–2025 | City-level municipality only |
| Avg historical transactions/quarter | 702 | 9,401 | — |
| Avg historical appraised €/m² | €2,043 | €3,616 | — |

**Ministerio limitation:** Both tables are at municipality level (one row per city per quarter). There is no neighborhood-level transaction data from Ministerio. These metrics are useful as macro market context (is the market growing? how does current supply compare to historical norms?) but cannot differentiate between neighborhoods.

**Idealista market dynamics (neighborhood level):**

| Metric | Available |
|---|---|
| priceDrop listings count | 315 (23% of total) |
| Listings with discount_pct | 319 rows |
| Avg discount on priceDrop listings | ~10–20% (varies) |
| Supply volume per neighborhood | Computable from existing data |

**Verdict: ⚠️ Partial**

- Neighborhood-level supply volume: ✅ computable from Idealista (but only 5 days)
- Neighborhood-level price drop ratio: ✅ computable (315 priceDrop listings, well distributed)
- Historical transaction trends: ⚠️ city-level only from Ministerio
- Market velocity (days on market): ❌ not available — would need listings to go inactive and we can't poll Idealista for status (HTTP 403)

---

## 4. Summary: Sub-score Feasibility Verdicts

| Sub-score | Verdict | Neighborhoods viable | Blocking issues |
|---|---|---|---|
| **Rental Yield** | ⚠️ Partial | 31 / 168 (18%) today, ~70% at 30 days | Need more collection time |
| **Affordability** | ⚠️ Partial | All neighborhoods (city-level only) | INE data is municipal, not sub-city |
| **Walkability** | ✅ Viable | 162–168 / 168 (excl. transport for Granada) | Granada transport gap |
| **Building Quality** | ✅ Viable | 146 / 168 (87%) | 22 neighborhoods with no Catastro data |
| **Market Dynamics** | ⚠️ Partial | 31 / 168 (price drop ratio); city-level for macro | No days-on-market data |

---

## 5. MVP Scoring Engine Recommendations

### 5.1 Include in MVP (Phase 3)

**Score 1: Walkability Index** (per neighborhood, all 162+ neighborhoods)
- Weighted average of: education POI density + health POI density + shopping POI density
- Normalize by neighborhood area_km2 (already in dim_neighborhoods)
- Transport: include for Madrid, exclude or flag for Granada
- Data ready: no additional ingestion needed

**Score 2: Building Age Score** (87% of neighborhoods)
- Average year_built per neighborhood from Catastro
- Invert: older avg year = lower score, normalize to 0–100
- Data ready: no additional ingestion needed

**Score 3: Listing Price per m² Benchmark** (partial, growing)
- For neighborhoods with ≥3 listings: compute median sale €/m² and rent €/m²
- Normalize within city (a neighborhood's price vs city median)
- Flag neighborhoods with insufficient data (<3 listings) as `N/A`
- Data growing: re-run monthly as volume builds

**Score 4: Price Drop Ratio** (partial)
- % of current listings that are priceDrop (vs newAd) per neighborhood
- Proxy for market pressure / seller motivation
- Computable now, but low confidence at 5 days

### 5.2 Defer to Phase 3.5+

**Rental Yield:** Wait until ≥4 weeks of Idealista data (~6×more listings). At 30 days, expect ~70% neighborhood coverage.

**Affordability:** Requires either (a) INE census-section data ingestion (complex new pipeline) or (b) accept city-level macro context only. Use city-level as a context card, not a neighborhood score.

**Market Velocity / Days-on-Market:** Blocked by Idealista's 403 anti-bot protection. Would require either long-term re-appearance tracking (6+ weeks) or a paid data source.

### 5.3 Pre-MVP Bug to Fix

**🐛 Madrid neighborhood name encoding corruption (31 names):** The `dim_neighborhoods` table and `stg_neighborhoods` view contain mojibake characters (`Ã­` instead of `í`, `Ã±` instead of `ñ`). Root cause: UTF-8 source treated as latin-1 during BQ load. Must fix in `neighborhoods.py` before building the dashboard — incorrect names will break any join or display that uses the name as a key.

---

## 6. Data Freshness Summary

| Table | Last loaded | Rows | Freshness |
|---|---|---|---|
| idealista_listings | Active (6h CF) | 1,376 | Live |
| osm_pois | 2026-03-15 | 8,060 | Static — annual refresh sufficient |
| catastro_buildings | 2026-03-15 | 72,684 | Static — annual refresh sufficient |
| ine_renta | 2026-03-15 | 3,120 | Annual (Oct); current = 2023 data |
| ine_ipv | 2026-03-15 | 608 | Quarterly |
| ministerio_transacciones | 2026-03-16 | 176 | Quarterly (manual XLS) |
| ministerio_valor_tasado | 2026-03-16 | 168 | Quarterly (manual XLS) |
| neighborhoods | 2026-03-17 | 197 | Static |
