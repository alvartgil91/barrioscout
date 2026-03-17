-- Neighbourhood and district polygons for spatial joins (ST_CONTAINS).
-- Geometry stored as WKT STRING in raw layer.
-- Clean/analytics layer converts to GEOGRAPHY via ST_GEOGFROMTEXT(geometry_wkt).

CREATE TABLE IF NOT EXISTS `portfolio-alvartgil91.barrioscout_raw.neighborhoods` (
  city           STRING     NOT NULL,   -- "Madrid" or "Granada"
  level          STRING     NOT NULL,   -- "district" or "neighborhood"
  name           STRING     NOT NULL,   -- e.g. "Palacio", "Centro"
  code           STRING,                -- e.g. "011" (Madrid), "BEI-01" (Granada), NULL for Granada districts
  district_name  STRING,                -- parent district name (NULL for district-level rows)
  geometry_wkt   STRING     NOT NULL,   -- WKT polygon (WGS84 / EPSG:4326)
  _loaded_at     TIMESTAMP  NOT NULL
);
