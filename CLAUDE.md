# BarrioScout — CLAUDE.md

## Project Purpose & Vision

BarrioScout is a real estate intelligence tool focused on the Spanish market (Granada and Madrid).
It collects data from public sources, calculates location and price scores by neighbourhood,
and presents investment opportunities through a Streamlit dashboard.

This is a professional portfolio project built by Alvaro (Senior Data Analyst / BI Engineer)
to demonstrate end-to-end data engineering skills on GCP.

---

## Architecture

```
Public Sources
  ├── Ministerio de Transportes  (quarterly transactions CSV)
  ├── Catastro INSPIRE           (property characteristics REST/XML)
  ├── OpenStreetMap Overpass     (POIs: schools, hospitals, supermarkets, metro, pharmacies)
  ├── Google Places API          (service ratings & reviews)
  └── INE                        (socioeconomic data: median income, population)
         │
         ▼
  Python ETL scripts (src/ingestion/)
         │
         ▼
  BigQuery raw layer  (barrioscout_raw)
         │
         ▼
  BigQuery clean layer (barrioscout_clean)
         │
         ▼
  BigQuery analytics layer (barrioscout_analytics)
         │
         ▼
  Streamlit Dashboard (src/app/)
```

---

## BigQuery Schema Structure

| Dataset                  | Purpose                                                   |
|--------------------------|-----------------------------------------------------------|
| `barrioscout_raw`        | Raw ingested data, minimal transformation, append-only    |
| `barrioscout_clean`      | Deduplicated, typed, validated data                       |
| `barrioscout_analytics`  | Aggregated scores, rankings, neighbourhood summaries      |

GCP Project: `portfolio-alvartgil91`

---

## Cities & Coordinates

| City    | Latitude  | Longitude  | Bounding Box (S, W, N, E)                  |
|---------|-----------|------------|---------------------------------------------|
| Granada | 37.1773   | -3.5986    | 37.1200, -3.6500, 37.2300, -3.5400          |
| Madrid  | 40.4168   | -3.7038    | 40.3100, -3.8300, 40.5600, -3.5200          |

---

## Code Conventions

- **Language**: All code, docstrings, comments, variable names, and commit messages in **English**
- **Type hints**: Required on all functions
- **Import order**: stdlib → third-party → local
- **Ingestion pattern**: Every ingestion script must expose three functions:
  - `extract() -> <raw_type>` — fetch data from source
  - `transform(raw) -> pd.DataFrame` — clean and normalise
  - `load(df: pd.DataFrame) -> None` — write to BigQuery raw layer

---

## Running the Project

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your API keys and GCP credentials

# 3. Validate all data sources (no BigQuery required)
python tests/test_sources.py

# 4. Run ingestion (example)
python -m src.ingestion.ministerio

# 5. Launch dashboard
streamlit run src/app/main.py
```

## Running Tests

```bash
# Source connectivity validation (no pytest, no BigQuery needed)
python tests/test_sources.py
```

---

## Cost Target

**0 €/month** — all GCP usage within free tier limits:
- BigQuery: < 10 GB storage, < 1 TB queries/month
- No Cloud Functions, no Scheduler (cron via local scripts)
