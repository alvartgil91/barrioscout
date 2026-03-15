"""
Ingestion module for Catastro INSPIRE API.

Source: REST API returning GML/XML with building and parcel data.
Docs: https://www.catastro.minhap.es/webinspire/index.html
Schema target: barrioscout_raw.catastro_buildings
"""

from __future__ import annotations

import logging
from xml.etree import ElementTree as ET

import pandas as pd
import requests

from config.settings import CATASTRO_INSPIRE_URL

logger = logging.getLogger(__name__)

# INSPIRE WFS endpoint for building data
CATASTRO_WFS_URL = (
    "https://ovc.catastro.meh.es/INSPIRE/wfsCP.aspx"
)


def extract(referencia_catastral: str) -> str:
    """Fetch building data for a given cadastral reference.

    Args:
        referencia_catastral: 20-character Spanish cadastral reference code.

    Returns:
        Raw XML response as a string.
    """
    params = {
        "SERVICE": "WFS",
        "REQUEST": "GetFeature",
        "TYPENAMES": "CP:CadastralParcel",
        "SRSName": "EPSG:4326",
        "CQL_FILTER": f"REFCAT='{referencia_catastral}'",
    }
    response = requests.get(CATASTRO_WFS_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.text


def transform(raw_xml: str) -> pd.DataFrame:
    """Parse the WFS XML response into a flat DataFrame.

    Args:
        raw_xml: Raw XML string from extract().

    Returns:
        DataFrame with parcel attributes.
    """
    root = ET.fromstring(raw_xml)
    ns = {
        "wfs": "http://www.opengis.net/wfs/2.0",
        "CP": "urn:x-inspire:specification:gmlas:CadastralParcels:3.0",
    }
    records: list[dict] = []
    for member in root.findall(".//CP:CadastralParcel", ns):
        record: dict = {}
        for child in member:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            record[tag] = child.text
        records.append(record)

    return pd.DataFrame(records) if records else pd.DataFrame()


def load(df: pd.DataFrame) -> None:
    """Load the transformed DataFrame into BigQuery raw layer.

    Args:
        df: Transformed DataFrame from transform().
    """
    from src.processing.bq_loader import load_dataframe
    load_dataframe(df, dataset="barrioscout_raw", table="catastro_buildings")
