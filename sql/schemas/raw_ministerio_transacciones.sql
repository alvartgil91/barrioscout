CREATE TABLE IF NOT EXISTS `portfolio-alvartgil91.barrioscout_raw.ministerio_transacciones` (
  municipality  STRING    NOT NULL,  -- Municipality name (e.g. "Granada", "Madrid")
  year          INT64     NOT NULL,  -- Calendar year (2004-2025)
  quarter       INT64     NOT NULL,  -- Quarter (1-4)
  transactions  INT64,               -- Number of property transactions; source: col 3-90 of XLS
  _loaded_at    TIMESTAMP NOT NULL   -- UTC timestamp when this row was ingested
);
