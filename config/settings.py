"""
Project-wide configuration: cities, coordinates, GCP settings, BigQuery dataset names.
No hardcoded values in ingestion scripts — import from here instead.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# GCP / BigQuery
# ---------------------------------------------------------------------------

GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "portfolio-alvartgil91")

BQ_DATASET_RAW: str = "barrioscout_raw"
BQ_DATASET_CLEAN: str = "barrioscout_clean"
BQ_DATASET_ANALYTICS: str = "barrioscout_analytics"

# ---------------------------------------------------------------------------
# Cities
# ---------------------------------------------------------------------------

CITIES: dict[str, dict] = {
    "granada": {
        "name": "Granada",
        "lat": 37.1773,
        "lon": -3.5986,
        # Overpass bounding box: south, west, north, east
        "bbox": (37.1200, -3.6500, 37.2300, -3.5400),
        # INE municipality code
        "ine_municipio": "18087",
        # Catastro province code
        "catastro_provincia": "18",
    },
    "madrid": {
        "name": "Madrid",
        "lat": 40.4168,
        "lon": -3.7038,
        "bbox": (40.3100, -3.8300, 40.5600, -3.5200),
        "ine_municipio": "28079",
        "catastro_provincia": "28",
    },
}

# ---------------------------------------------------------------------------
# External API endpoints
# ---------------------------------------------------------------------------

OVERPASS_URL: str = "https://overpass-api.de/api/interpreter"

CATASTRO_INSPIRE_URL: str = (
    "https://ovc.catastro.meh.es/OVCServWeb/OVCWcfLibres/OVCFotoFachada.svc/rest"
)

# INE — Índice de Precios de Vivienda (IPV), quarterly, by CCAA
# NOTE: The Ministerio de Transportes (transportes.gob.es) publishes more granular
# municipal-level data but blocks programmatic access via CloudFront WAF.
# The INE IPV series is a reliable open alternative for price trend analysis.
INE_IPV_URL: str = "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/25171.csv"

# Ministerio de Transportes base URL (manual download only — WAF blocks scripts)
MINISTERIO_BASE_URL: str = (
    "https://www.transportes.gob.es/recursos_mfom/paginabasica/recursos/"
)

# INE — Renta neta media por persona (Atlas de distribución de renta)
INE_RENTA_URL: str = (
    "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/30896.csv"
)

# Google Places
GOOGLE_PLACES_API_KEY: str = os.getenv("GOOGLE_PLACES_API_KEY", "")
GOOGLE_PLACES_URL: str = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

# ---------------------------------------------------------------------------
# OSM POI categories (Overpass amenity / shop tags)
# ---------------------------------------------------------------------------

OSM_POI_TAGS: dict[str, list[str]] = {
    "education": ["school", "college", "university", "kindergarten"],
    "health": ["hospital", "clinic", "pharmacy", "doctors"],
    "transport": ["subway_entrance", "bus_station", "train_station"],
    "shopping": ["supermarket", "marketplace"],
}
