-- Schema for barrioscout_raw.catastro_buildings
-- Source: Catastro INSPIRE WFS — wfsBU.aspx (Buildings), operación GetFeature
-- Grain: one row per building (unique cadastral reference)
-- Load mode: WRITE_APPEND (append-only raw layer)

CREATE TABLE IF NOT EXISTS `portfolio-alvartgil91.barrioscout_raw.catastro_buildings` (
  cadastral_ref  STRING    NOT NULL,  -- Spanish cadastral reference; source: bu-core2d:reference
  year_built     INT64,               -- Year of construction; source: bu-core2d:beginning (first 4 chars)
  current_use    STRING,              -- e.g. "1_residential", "3_industrial"; source: bu-ext2d:currentUse
  latitude       FLOAT64,             -- WGS84 centroid latitude (midpoint of EPSG:25830 bounding box)
  longitude      FLOAT64,             -- WGS84 centroid longitude (midpoint of EPSG:25830 bounding box)
  _loaded_at     TIMESTAMP NOT NULL   -- UTC timestamp when this row was ingested
);
