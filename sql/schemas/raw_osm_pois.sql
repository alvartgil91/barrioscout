CREATE TABLE IF NOT EXISTS `portfolio-alvartgil91.barrioscout_raw.osm_pois` (
  osm_id    INT64     NOT NULL,  -- OSM element ID
  city      STRING    NOT NULL,  -- City key (e.g. "granada", "madrid"); source: CITIES config
  category  STRING    NOT NULL,  -- POI category (e.g. "health", "transport"); source: OSM_POI_TAGS key
  osm_type  STRING,              -- Concrete tag value matched (e.g. "hospital", "subway_entrance")
  name      STRING,              -- POI name from OSM tags (nullable)
  lat       FLOAT64   NOT NULL,  -- WGS84 latitude; nodes: top-level lat, ways/relations: center.lat
  lon       FLOAT64   NOT NULL,  -- WGS84 longitude; nodes: top-level lon, ways/relations: center.lon
  _loaded_at TIMESTAMP NOT NULL  -- UTC timestamp when this row was ingested
);
