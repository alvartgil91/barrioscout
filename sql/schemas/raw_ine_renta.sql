-- Schema for barrioscout_raw.ine_renta
-- Source: INE Atlas de distribución de renta de los hogares (table 30896)
-- Grain: one row per municipality × year
-- Load mode: WRITE_APPEND (append-only raw layer)

CREATE TABLE IF NOT EXISTS `portfolio-alvartgil91.barrioscout_raw.ine_renta` (
  municipality_code  STRING    NOT NULL,  -- 5-digit INE code; source: "Municipios" (prefix)
  municipality_name  STRING,              -- Municipality name; source: "Municipios" (suffix)
  year               INT64     NOT NULL,  -- Reference year; source: "Periodo"
  net_avg_income     FLOAT64,             -- Net avg income per capita (EUR); source: "Renta neta media por persona"
  city               STRING,              -- Derived label: "Granada" (prov 18) | "Madrid" (prov 28) | NULL
  _loaded_at         TIMESTAMP NOT NULL   -- UTC timestamp when this row was ingested
);
