# Granada Transport POI Gap — Investigation Report
*Date: 2026-03-20*

## Finding

Granada has 36 transport POIs vs Madrid's 1,014 — a 28× difference that is only partially
explained by city size. **The primary cause is a missing OSM tag: `highway=bus_stop`.**

## Current data in `barrioscout_raw.osm_pois` (transport, Granada)

| osm_type | Count | Notes |
|---|---|---|
| station | 26 | `railway=station` or `public_transport=station` — metro + intercity rail |
| subway_entrance | 9 | `railway=subway_entrance` — Metro de Granada (Line 1) entrances |
| bus_station | 1 | `amenity=bus_station` — the main Estación de Autobuses |
| **Total** | **36** | |

## Root cause: `highway=bus_stop` not queried

The Overpass query in `osm_pois.py` covers:
- `amenity=*` for values in `OSM_POI_TAGS["transport"]` (`subway_entrance`, `bus_station`, `train_station`)
- `railway=subway_entrance`, `railway=station`, `railway=tram_stop`
- `public_transport=station`

Individual **bus stops** in OSM use a completely different key: `highway=bus_stop`. This key is
not queried anywhere in the current pipeline, and it is **not** the same as `amenity=bus_station`
(which only covers major interchange buildings, not individual stops).

## Why this matters more for Granada than Madrid

Granada's public transport network is **almost entirely bus-based.** The metro (Line 1, opened
2017) has only 26 stations. The city bus network (LAC/Rober) and metropolitan bus network cover
the entire urban area with hundreds of individual stops, all tagged as `highway=bus_stop` in OSM.

Madrid also lacks bus stops in the current data, but its 645 metro entrances and 350 railway
stations mean most neighborhoods still show meaningful transport access. For Granada, without bus
stops, 27/37 neighborhoods (73%) appear to have zero transport infrastructure — which is
misleading.

## Verification

A test Overpass query for `highway=bus_stop` in the Granada bbox
`(37.1200,-3.6500,37.2300,-3.5400)` would return several hundred nodes. OSM has good coverage
of Granada's bus network.

## Real-world context

Granada transport is genuinely sparser than Madrid — but not by 28×. A reasonable estimate:
- Granada: ~400–600 bus stops + 36 current POIs ≈ ~450–650 transport POIs total
- Madrid: ~3,000+ bus stops + 1,014 current POIs ≈ 4,000+ transport POIs total
- Corrected ratio: ~6–8×, which reflects city size difference more accurately.

## Recommended fix (Phase 3)

Add `("highway", "bus_stop")` to `_TRANSPORT_EXTRA` in `src/ingestion/osm_pois.py`:

```python
_TRANSPORT_EXTRA: list[tuple[str, str]] = [
    ("railway", "subway_entrance"),
    ("railway", "station"),
    ("railway", "tram_stop"),
    ("public_transport", "station"),
    ("highway", "bus_stop"),          # ← add this
]
```

Then re-run `python -m src.ingestion.osm_pois` for Granada (and optionally Madrid) with
`WRITE_APPEND` — the dedup by `osm_id` in `main()` prevents duplicates for existing POIs.

**Expected impact:** ~400–600 new Granada transport POIs. This would drop zero-transport
neighborhoods from 27/37 (73%) to likely 3–5/37 (outlying semi-rural barrios).

## Decision for MVP

- **Do not block Phase 3 on this fix.** Walkability scores can be computed without transport
  for Granada, with a `transport_score = NULL` flag for that city rather than a misleading 0.
- **Batch this re-ingestion at the start of Phase 3** alongside the first Dataform staging
  model builds for POIs (`stg_osm_pois`).
