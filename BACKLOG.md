# BarrioScout Backlog

## Requires scheduler reactivation first

- **Restore osm_pois**: Run `python -m src.ingestion.osm_pois` when Overpass API recovers (last attempt: HTTP 406). Analytics layer (`int_neighborhood_pois`) is intact.
- **Activate DOM in scoring**: Add `dom_prank` to `market_dynamics_score` when the range between neighborhoods exceeds 30 days in any city (currently 2–9 days globally).
- **Investigate price/yield NULLs**: Some zones with visible listings still show NULL `median_sale_price_m2` or `gross_rental_yield_pct` — check neighborhood assignment in `fct_listing_observations`.

## Data quality

- **~420 orphan listings**: Listings that cannot be spatially joined to any polygon (metro municipalities below subdivision threshold). Two-pass join covers 200m fallback but smaller municipalities are unpolygoned.
- **neighborhoods table has 272 rows vs 290 expected**: Some Tier 1–4 metro municipality subdivisions are missing. See `scripts/subdivision_strategy.md` for the 21 municipalities and their current tier status.
- **241 scored zones vs 350 target**: Directly caused by the incomplete subdivisions above.

## Infrastructure

- **Add monitoring for Cloud Function failures**: Silent breakage ran 7 weeks undetected. Add Cloud Monitoring alert on function error rate or email volume drop.
- **Add OAuth token expiry alert**: Gmail refresh token will expire if app stays in "testing" mode. Alert before token expires so ingestion doesn't silently stop.
- **Fix Majadahonda all-zero scores**: Low listing count + scoring thresholds may be filtering it out of all sub-scores.

## Parked (museum mode — needs active pipeline first)

- **Liquidity score** using `listing_status_checks` data (clicks, contacts, deactivations per listing)
- **Subdivision of large municipalities**: Alcalá de Henares (~28 listings), Las Gabias (~30 listings) — large enough to subdivide, low enough priority to defer
- **Ingest INE census data for metro municipalities**: Currently no sub-city income differentiation for metro zones
- **AI-generated neighborhood summaries** via Claude batch API (precalculated, stored in BQ)
