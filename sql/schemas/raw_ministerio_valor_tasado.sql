CREATE TABLE IF NOT EXISTS `portfolio-alvartgil91.barrioscout_raw.ministerio_valor_tasado` (
  province               STRING    NOT NULL,  -- Province name; source: col 1 of each sheet (forward-filled)
  municipality           STRING    NOT NULL,  -- Municipality name (>25K inhabitants); source: col 2
  year                   INT64     NOT NULL,  -- Calendar year parsed from sheet name (e.g. "T1A2005" → 2005)
  quarter                INT64     NOT NULL,  -- Quarter parsed from sheet name (e.g. "T1A2005" → 1)
  appraised_value_eur_m2 FLOAT64,             -- Mean appraised value €/m²; "n.r" in source → NULL
  num_appraisals         FLOAT64,             -- Number of appraisals; "n.r" in source → NULL
  _loaded_at             TIMESTAMP NOT NULL   -- UTC timestamp when this row was ingested
);
