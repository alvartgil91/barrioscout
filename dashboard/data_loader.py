"""BigQuery data loading functions for the BarrioScout dashboard.

Auth strategy:
- Deployment (Streamlit Cloud): reads credentials from st.secrets["gcp_service_account"]
- Local dev: falls back to Application Default Credentials (gcloud auth application-default login)
"""

import json
from typing import Optional


def _normalize_geometry(geometry: Optional[dict]) -> Optional[dict]:
    """Flatten a GeoJSON GeometryCollection into a MultiPolygon.

    BigQuery's ST_ASGEOJSON can return a GeometryCollection for some
    neighbourhood polygons.  Folium 0.20 crashes on this type because its
    iter_coords() directly accesses ["coordinates"], which GeometryCollection
    does not have (it has ["geometries"] instead).

    Any non-Collection geometry is returned unchanged.
    Any Collection whose member geometries are all Polygon/MultiPolygon is
    collapsed into a single MultiPolygon.  Collections that contain no polygon
    geometries are returned as None so the feature is dropped from the map.
    """
    if geometry is None:
        return None
    if geometry.get("type") != "GeometryCollection":
        return geometry

    polygon_rings: list = []
    for geom in geometry.get("geometries", []):
        gtype = geom.get("type")
        if gtype == "Polygon":
            polygon_rings.append(geom["coordinates"])
        elif gtype == "MultiPolygon":
            polygon_rings.extend(geom["coordinates"])
        # Point / LineString members are dropped — not meaningful for choropleth

    if not polygon_rings:
        return None
    return {"type": "MultiPolygon", "coordinates": polygon_rings}

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

GCP_PROJECT = "portfolio-alvartgil91"
DATASET_ANALYTICS = f"{GCP_PROJECT}.barrioscout_analytics"
DATASET_STAGING   = f"{GCP_PROJECT}.barrioscout_staging"


def _get_bq_client() -> bigquery.Client:
    """Return an authenticated BigQuery client.

    Uses Streamlit secrets when available (Streamlit Cloud deployment),
    otherwise falls back to Application Default Credentials for local dev.
    """
    try:
        sa_info = st.secrets["gcp_service_account"]
        credentials = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=GCP_PROJECT, credentials=credentials)
    except (KeyError, FileNotFoundError):
        # Local dev: no secrets file — relies on Application Default Credentials
        # (`gcloud auth application-default login`)
        return bigquery.Client(project=GCP_PROJECT)


@st.cache_data(ttl=86400)
def load_neighborhood_scores(metro_area: str) -> pd.DataFrame:
    """Return the full scoring card for every neighborhood in *metro_area*.

    Includes city neighborhoods AND metropolitan municipalities.

    Columns returned (from agg_neighborhood_scores):
        neighborhood_id, neighborhood_name, city, metro_area,
        district_id, district_name, area_km2,
        health_count, education_count, shopping_count, transport_count,
        total_pois, pois_per_km2,
        residential_buildings, avg_year_built, median_year_built,
        pct_post_2000, pct_pre_1960,
        sale_count, rent_count, total_listings, pricedrop_ratio,
        median_sale_price_m2, median_rent_price_m2, gross_rental_yield_pct,
        services_score, building_quality_score, price_score,
        yield_score, market_dynamics_score,
        composite_score, data_completeness, available_sub_scores, scored_at

    Args:
        metro_area: Metro area name, e.g. "Madrid" or "Granada".

    Returns:
        DataFrame with one row per neighborhood.
    """
    # zone_type and parent_municipality are sourced from stg_neighborhoods, a
    # Dataform VIEW (barrioscout_staging) that is always current — it derives
    # these fields from barrioscout_raw.neighborhoods on every query execution.
    # The materialized tables (dim_neighborhoods, agg_neighborhood_scores) may
    # not yet have these columns if the Dataform pipeline has not been re-run.
    query = f"""
        SELECT
            s.neighborhood_id,
            s.neighborhood_name,
            s.city,
            s.metro_area,
            s.district_id,
            s.district_name,
            s.area_km2,
            s.health_count,
            s.education_count,
            s.shopping_count,
            s.transport_count,
            s.total_pois,
            s.pois_per_km2,
            s.residential_buildings,
            s.avg_year_built,
            s.median_year_built,
            s.pct_post_2000,
            s.pct_pre_1960,
            s.sale_count,
            s.rent_count,
            s.total_listings,
            s.pricedrop_ratio,
            s.median_sale_price_m2,
            s.median_rent_price_m2,
            s.gross_rental_yield_pct,
            s.services_score,
            s.building_quality_score,
            s.price_score,
            s.yield_score,
            s.market_dynamics_score,
            s.composite_score,
            s.data_completeness,
            s.available_sub_scores,
            s.scored_at,
            sn.zone_type,
            sn.parent_municipality
        FROM `{DATASET_ANALYTICS}.agg_neighborhood_scores` AS s
        LEFT JOIN `{DATASET_STAGING}.stg_neighborhoods` AS sn
            ON sn.area_id = s.neighborhood_id
            AND sn.level = 'neighborhood'
        WHERE LOWER(s.metro_area) = LOWER(@metro_area)
        ORDER BY s.composite_score DESC NULLS LAST
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("metro_area", "STRING", metro_area)]
    )
    client = _get_bq_client()
    return client.query(query, job_config=job_config).to_dataframe()


@st.cache_data(ttl=86400)
def load_neighborhood_geometries(metro_area: str) -> dict:
    """Return a GeoJSON FeatureCollection for every neighborhood in *metro_area*.

    Includes city neighborhoods AND metropolitan municipalities.
    Converts the GEOGRAPHY column to GeoJSON via ST_ASGEOJSON so it can be
    consumed directly by Folium / any GeoJSON-aware library.

    Each Feature's properties include:
        neighborhood_id, neighborhood_name, city,
        district_id, district_name, area_km2

    Args:
        metro_area: Metro area name, e.g. "Madrid" or "Granada".

    Returns:
        GeoJSON FeatureCollection dict.
    """
    query = f"""
        SELECT
            neighborhood_id,
            neighborhood_name,
            city,
            district_id,
            district_name,
            area_km2,
            ST_ASGEOJSON(geometry) AS geometry_geojson
        FROM `{DATASET_ANALYTICS}.dim_neighborhoods`
        WHERE LOWER(metro_area) = LOWER(@metro_area)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("metro_area", "STRING", metro_area)]
    )
    client = _get_bq_client()
    df = client.query(query, job_config=job_config).to_dataframe()

    features = []
    for _, row in df.iterrows():
        raw_geom = json.loads(row["geometry_geojson"]) if row["geometry_geojson"] else None
        geometry = _normalize_geometry(raw_geom)
        features.append(
            {
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "neighborhood_id": row["neighborhood_id"],
                    "neighborhood_name": row["neighborhood_name"],
                    "city": row["city"],
                    "district_id": row["district_id"],
                    "district_name": row["district_name"],
                    "area_km2": row["area_km2"],
                },
            }
        )

    return {"type": "FeatureCollection", "features": features}


@st.cache_data(ttl=86400)
def load_listings(neighborhood_id: Optional[str] = None) -> pd.DataFrame:
    """Return listings from int_listings_latest, optionally filtered by neighborhood.

    Columns returned:
        property_id, neighborhood_id, neighborhood_city,
        operation_type, price, price_per_m2, area_m2, bedrooms,
        campaign_type, property_url,
        first_seen_at, last_seen_at, times_seen,
        has_price_drop, days_on_market

    Args:
        neighborhood_id: Optional neighborhood ID to filter results.
                         When None, returns all listings (may be large).

    Returns:
        DataFrame with one row per property (latest snapshot).
    """
    where_clause = "WHERE neighborhood_id = @neighborhood_id" if neighborhood_id is not None else ""

    query = f"""
        SELECT
            property_id,
            neighborhood_id,
            neighborhood_city,
            operation_type,
            price,
            price_per_m2,
            area_m2,
            bedrooms,
            campaign_type,
            property_url,
            first_seen_at,
            last_seen_at,
            times_seen,
            has_price_drop,
            days_on_market
        FROM `{DATASET_ANALYTICS}.int_listings_latest`
        {where_clause}
        ORDER BY last_seen_at DESC
    """

    query_params = []
    if neighborhood_id is not None:
        query_params.append(
            bigquery.ScalarQueryParameter("neighborhood_id", "STRING", neighborhood_id)
        )

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    client = _get_bq_client()
    return client.query(query, job_config=job_config).to_dataframe()
