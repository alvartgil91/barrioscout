-- Schema for barrioscout_raw.ine_renta
-- Source: INE Atlas de distribución de renta de los hogares (table 30896)
-- Grain: one row per municipality × year
-- Load mode: WRITE_APPEND (append-only raw layer)

CREATE TABLE IF NOT EXISTS `portfolio-alvartgil91.barrioscout_raw.ine_renta` (
  municipio_codigo  STRING    NOT NULL,  -- 5-digit INE municipality code (e.g. "18087")
  municipio_nombre  STRING,              -- Municipality name (e.g. "Granada, ciudad")
  año               INT64     NOT NULL,  -- Reference year
  renta_neta_media  FLOAT64,             -- Net median income per capita (EUR)
  city              STRING,              -- Derived city label: "Granada" | "Madrid"
  _loaded_at        TIMESTAMP NOT NULL   -- UTC timestamp when this row was ingested
);
