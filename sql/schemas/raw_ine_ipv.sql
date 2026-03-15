-- Schema for barrioscout_raw.ine_ipv
-- Source: INE Índice de Precios de Vivienda (table 25171)
-- Grain: one row per autonomous_community × index_type × quarter
-- Load mode: WRITE_APPEND (append-only raw layer)

CREATE TABLE IF NOT EXISTS `portfolio-alvartgil91.barrioscout_raw.ine_ipv` (
  autonomous_community  STRING    NOT NULL,  -- CCAA name; source: "Comunidades y Ciudades Autónomas"
  index_type            STRING    NOT NULL,  -- e.g. "Índice", "Tasa anual"; source: "Índices y tasas"
  period                STRING    NOT NULL,  -- Quarter in "YYYYTn" format; source: "Periodo"
  value                 FLOAT64,             -- Index value or rate; source: "Total"
  _loaded_at            TIMESTAMP NOT NULL   -- UTC timestamp when this row was ingested
);
