"""
Ingestion module for Idealista property listings via Gmail email alerts.

Reads emails from configured Idealista senders, parses property cards from HTML,
geocodes addresses with Nominatim, and loads to BigQuery.

Schema target: barrioscout_raw.idealista_listings

Dependencies: google-auth-oauthlib, google-api-python-client, beautifulsoup4,
              requests, pandas
"""

from __future__ import annotations

import base64
import re
import time
from email.utils import parsedate_to_datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.discovery import Resource

from config.settings import (
    GMAIL_CREDENTIALS_PATH,
    GMAIL_SCOPES,
    GMAIL_TOKEN_PATH,
    IDEALISTA_EMAIL_SENDERS,
    NOMINATIM_URL,
    NOMINATIM_USER_AGENT,
)


# ---------------------------------------------------------------------------
# Gmail authentication
# ---------------------------------------------------------------------------


def get_gmail_service(creds: Credentials | None = None) -> Resource:
    """Authenticate with Gmail API and return a service resource.

    If *creds* is provided (e.g. from Secret Manager in Cloud Functions),
    they are used directly — no local files or browser flow involved.

    Otherwise, falls back to the local token file + browser OAuth2 flow
    (original behaviour for local development).

    Args:
        creds: Pre-built OAuth2 credentials. When ``None``, credentials
               are loaded from local files.

    Returns:
        Authenticated Gmail API service resource.
    """
    if creds is None:
        token_path = Path(GMAIL_TOKEN_PATH)
        credentials_path = Path(GMAIL_CREDENTIALS_PATH)

        if token_path.exists():
            creds = Credentials.from_authorized_user_file(str(token_path), GMAIL_SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(credentials_path), GMAIL_SCOPES
                )
                creds = flow.run_local_server(port=0)
            token_path.write_text(creds.to_json())

    return build("gmail", "v1", credentials=creds)


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------


def _nominatim_query(q: str) -> tuple[float, float] | None:
    """Single Nominatim request. Sleeps 1.1s afterwards (rate limit). Returns (lat, lon) or None."""
    try:
        response = requests.get(
            NOMINATIM_URL,
            params={"q": q, "format": "json", "limit": 1, "countrycodes": "es"},
            headers={"User-Agent": NOMINATIM_USER_AGENT},
            timeout=10,
        )
        response.raise_for_status()
        results = response.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as exc:
        print(f"  [geocode] Request failed for '{q}': {exc}")
    finally:
        time.sleep(1.1)
    return None


def geocode_address(address: str, city: str) -> tuple[float | None, float | None]:
    """Geocode a Spanish property address using Nominatim with progressive fallback.

    Attempt 1: full address as-is + countrycodes=es
    Attempt 2: first segment of address (street + number) + city + España
    Attempt 3: city + España only

    Respects Nominatim ToS: 1.1s sleep after every request.

    Args:
        address: Street address (e.g. "Calle de Alcántara, Goya, Madrid").
        city: City name for disambiguation (e.g. "Madrid").

    Returns:
        (lat, lon) or (None, None) if all attempts fail.
    """
    city_lower = city.lower().strip()

    # Attempt 1: full address. Skip appending city if already present.
    if city_lower in address.lower():
        q1 = address
    else:
        q1 = f"{address}, {city}"
    result = _nominatim_query(q1)
    if result:
        return result

    # Attempt 2: first comma-segment (street + number) + city + España
    first_segment = address.split(",")[0].strip()
    q2 = f"{first_segment}, {city}, España"
    result = _nominatim_query(q2)
    if result:
        return result

    # Attempt 3: city + España
    q3 = f"{city}, España"
    result = _nominatim_query(q3)
    if result:
        return result

    return None, None


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------


_PROPERTY_TYPE_RE = re.compile(
    r"^(Piso|Chalet|Ático|Casa|Estudio|Dúplex|Apartamento|Local|Garaje|Trastero|Oficina)",
    re.IGNORECASE,
)


def _parse_price(text: str) -> tuple[float | None, str]:
    """Return (price_float, operation_type) from a raw price string.

    Extracts only the digits immediately before the € symbol to avoid
    capturing unrelated numbers (e.g. '↓18%' in priceDrop emails).
    Spanish thousands separator is '.', so '280.000' → 280000.
    """
    operation_type = "rent" if "/mes" in text else "sale"
    # Match digits (with optional . thousands separators) immediately before €
    m = re.search(r"([\d.]+)\s*€", text)
    if m:
        cleaned = m.group(1).replace(".", "")
        try:
            return float(cleaned), operation_type
        except ValueError:
            pass
    return None, operation_type


def parse_listings_from_email(soup: BeautifulSoup) -> list[dict]:
    """Parse all property listings from an Idealista email.

    Idealista emails use table-based HTML where property data (image, title,
    price, features) is spread across different table cells.  The most reliable
    anchors are:
      - <a href=".../inmueble/NNNNNN/..."> → property_id + image_url
      - <img title="Piso en Calle X, Barrio, City"> → property_type + address
      - <span>1.350 €/mes</span> → price + operation_type
      - <td> 55 m² 2 hab. 2ª planta </td> → area / bedrooms / floor / is_exterior

    For emails with multiple properties (daily/remarketing format), each
    property has its own <a href="/inmueble/..."> containing an <img title>.
    We match price/features to each property by scanning the elements that
    follow the property link in document order.

    Args:
        soup: Parsed BeautifulSoup of the full email HTML.

    Returns:
        List of listing dicts (one per property found in the email).
    """
    # Collect all property anchors (deduped by property_id)
    anchors: list[tuple[str, str | None, str | None, str | None]] = []
    seen_ids: set[str] = set()

    for a_tag in soup.find_all("a", href=True):
        m = re.search(r"/inmueble/(\d+)/", a_tag["href"])
        if not m:
            continue
        prop_id = m.group(1)
        if prop_id in seen_ids:
            continue
        seen_ids.add(prop_id)

        # Image and title inside the <a>
        img = a_tag.find("img", title=_PROPERTY_TYPE_RE)
        image_url: str | None = None
        raw_title: str | None = None
        if img:
            image_url = img.get("src")
            raw_title = img.get("title", "")
        else:
            img = a_tag.find("img")
            if img:
                image_url = img.get("src")

        anchors.append((prop_id, image_url, raw_title, a_tag["href"]))

    if not anchors:
        return []

    # Collect price spans and feature tds from the whole document
    price_texts: list[str] = []
    for tag in soup.find_all(["span", "td", "div"]):
        text = tag.get_text(" ").strip()
        if "€" in text and len(text) < 30 and re.search(r"\d", text):
            price_texts.append(text)

    feature_texts: list[str] = []
    for tag in soup.find_all(["td", "div", "span"]):
        text = tag.get_text(" ").strip()
        if "m²" in text and len(text) < 150:
            feature_texts.append(text)

    # Description: longest paragraph-ish block without price/feature markers
    description: str | None = None
    for tag in soup.find_all(["p", "div", "td"]):
        text = tag.get_text(" ").strip()
        if (
            len(text) > 100
            and "€" not in text
            and "m²" not in text
            and "hab." not in text
            and "Hola" not in text
            and "Ver todos" not in text
        ):
            description = text[:500]
            break

    listings: list[dict] = []
    for idx, (prop_id, image_url, raw_title, _href) in enumerate(anchors):
        # --- property_type + address from img title ---
        property_type: str | None = None
        address: str | None = None
        if raw_title:
            # Format: "Piso en Calle X, Barrio, City" or "Piso en Barrio, City"
            title_match = re.match(
                r"(Piso|Chalet|Ático|Casa|Estudio|Dúplex|Apartamento|Local|Garaje|Trastero|Oficina)"
                r"\s+en\s+(.+)",
                raw_title,
                re.IGNORECASE,
            )
            if title_match:
                property_type = title_match.group(1).strip().capitalize()
                address = title_match.group(2).strip()

        # --- price: pick the idx-th price (one per property in multi-card emails) ---
        price: float | None = None
        operation_type: str = "sale"
        if idx < len(price_texts):
            price, operation_type = _parse_price(price_texts[idx])
        # Also check campaign hint from href if price parsing is ambiguous
        if "rent" in _href and operation_type == "sale":
            operation_type = "rent"

        # --- features ---
        area_m2: float | None = None
        bedrooms: int | None = None
        floor: int | None = None
        is_exterior: bool = False
        if idx < len(feature_texts):
            ft = feature_texts[idx]
            m2 = re.search(r"(\d+)\s*m²", ft)
            if m2:
                area_m2 = float(m2.group(1))
            bed = re.search(r"(\d+)\s*hab", ft)
            if bed:
                bedrooms = int(bed.group(1))
            fl = re.search(r"(\d+)[\wª]?\s*planta", ft)
            if fl:
                floor = int(fl.group(1))
            is_exterior = "exterior" in ft.lower()

        listings.append(
            {
                "property_id": prop_id,
                "operation_type": operation_type,
                "property_type": property_type,
                "address": address,
                "price": price,
                "area_m2": area_m2,
                "bedrooms": bedrooms,
                "floor": floor,
                "is_exterior": is_exterior,
                "description": description,
                "image_url": image_url,
            }
        )

    return listings


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------


def _decode_body(payload: dict) -> str | None:
    """Recursively extract text/html body from a Gmail message payload."""
    mime_type = payload.get("mimeType", "")
    body_data = payload.get("body", {}).get("data")

    if mime_type == "text/html" and body_data:
        return base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")

    for part in payload.get("parts", []):
        result = _decode_body(part)
        if result:
            return result

    return None


def _extract_campaign_type(soup: BeautifulSoup) -> str | None:
    """Extract utm_campaign value from any link in the email."""
    for a_tag in soup.find_all("a", href=True):
        match = re.search(r"utm_campaign=([^&\"'\s]+)", a_tag["href"])
        if match:
            return match.group(1)
    return None


def extract(
    max_emails: int = 200,
    creds: Credentials | None = None,
) -> list[dict]:
    """Fetch Idealista alert emails from Gmail and parse property listings.

    Searches for unprocessed emails from Idealista senders (those without the
    'BarrioScout/Procesado' label). Parses all property cards from HTML bodies.

    Args:
        max_emails: Maximum number of emails to process per run. Keeps execution
                    time bounded (especially for geocoding). Remaining emails
                    will be picked up in the next run.
        creds: Pre-built OAuth2 credentials forwarded to ``get_gmail_service``.

    Returns:
        List of dicts, one per property listing found across all emails.
    """
    service = get_gmail_service(creds=creds)

    # Build search query: emails from Idealista senders, not yet processed
    # in:anywhere includes spam/trash/archive in addition to inbox
    sender_query = " OR ".join(f"from:{s}" for s in IDEALISTA_EMAIL_SENDERS)
    query = f"in:anywhere ({sender_query}) -label:BarrioScout/Procesado"

    print(f"Searching Gmail: {query}")
    response = service.users().messages().list(userId="me", q=query, maxResults=200).execute()
    messages = response.get("messages", [])
    print(f"Found {len(messages)} emails to process")

    # Cap the number of emails to process per run
    if len(messages) > max_emails:
        print(f"Capping to {max_emails} emails (remaining will be processed next run)")
        messages = messages[:max_emails]

    listings: list[dict] = []

    for msg_meta in messages:
        msg_id = msg_meta["id"]
        msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()

        # Extract email date
        headers = {h["name"]: h["value"] for h in msg["payload"].get("headers", [])}
        date_str = headers.get("Date", "")
        try:
            email_date = parsedate_to_datetime(date_str)
        except Exception:
            email_date = None

        # Decode HTML body
        html_body = _decode_body(msg["payload"])
        if not html_body:
            print(f"  [skip] No HTML body in message {msg_id}")
            continue

        soup = BeautifulSoup(html_body, "lxml")
        campaign_type = _extract_campaign_type(soup)

        email_listings = parse_listings_from_email(soup)
        for listing in email_listings:
            listing["email_id"] = msg_id
            listing["email_date"] = email_date
            listing["campaign_type"] = campaign_type
            listings.append(listing)

        print(f"  [msg {msg_id}] campaign={campaign_type} → {len(email_listings)} listings")

    return listings


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def transform(listings: list[dict]) -> pd.DataFrame:
    """Clean and enrich raw listing dicts into a typed DataFrame.

    Steps:
    - Cast numeric columns (price, area_m2, bedrooms, floor)
    - Extract city from address (last comma-separated segment)
    - Geocode each listing with Nominatim
    - Deduplicate by (property_id, email_id)

    Args:
        listings: Raw listing dicts from extract().

    Returns:
        Typed DataFrame ready for BigQuery load.
    """
    df = pd.DataFrame(listings)

    # Cast numerics
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["area_m2"] = pd.to_numeric(df["area_m2"], errors="coerce")
    df["bedrooms"] = pd.to_numeric(df["bedrooms"], errors="coerce").astype("Int64")
    df["floor"] = pd.to_numeric(df["floor"], errors="coerce").astype("Int64")
    df["is_exterior"] = df["is_exterior"].fillna(False).astype(bool)

    # Extract city from address (last segment after final comma)
    def _extract_city(addr: object) -> str | None:
        if not addr or not isinstance(addr, str):
            return None
        parts = [p.strip() for p in addr.split(",")]
        return parts[-1] if parts else None

    df["city"] = df["address"].apply(_extract_city)

    # Drop listings without city — can't assign neighbourhood or score
    no_city_count = df["city"].isna().sum()
    if no_city_count:
        print(f"Dropping {no_city_count} listings without city")
        df = df.dropna(subset=["city"]).reset_index(drop=True)

    # Dedup by (property_id, email_id)
    df = df.drop_duplicates(subset=["property_id", "email_id"]).reset_index(drop=True)

    # Geocode
    print(f"Geocoding {len(df)} listings (Nominatim, ~1.1s each)...")
    lats, lons = [], []
    for _, row in df.iterrows():
        if row["address"] and row["city"]:
            lat, lon = geocode_address(row["address"], row["city"])
        else:
            lat, lon = None, None
        lats.append(lat)
        lons.append(lon)

    df["lat"] = lats
    df["lon"] = lons

    # Enforce column order and types for BigQuery
    df["property_id"] = df["property_id"].astype(str)
    df["operation_type"] = df["operation_type"].astype(str)
    df["email_id"] = df["email_id"].astype(str)

    columns = [
        "property_id", "operation_type", "property_type", "address", "city",
        "price", "area_m2", "bedrooms", "floor", "is_exterior", "description",
        "image_url", "lat", "lon", "email_date", "campaign_type", "email_id",
    ]
    # Only keep columns that exist (defensive)
    columns = [c for c in columns if c in df.columns]
    return df[columns]


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------


def load(df: pd.DataFrame) -> int:
    """Load transformed DataFrame into BigQuery raw layer.

    Args:
        df: Transformed listings DataFrame.

    Returns:
        Number of rows loaded.
    """
    from src.processing.bq_loader import load_to_bigquery

    return load_to_bigquery(df, "barrioscout_raw.idealista_listings")


# ---------------------------------------------------------------------------
# Post-process: label emails as processed
# ---------------------------------------------------------------------------


def post_process(
    email_ids: list[str],
    creds: Credentials | None = None,
) -> None:
    """Archive processed emails by adding a 'BarrioScout/Procesado' label.

    Creates the label if it does not exist. Removes INBOX label to archive.

    Args:
        email_ids: Gmail message IDs to mark as processed.
        creds: Pre-built OAuth2 credentials forwarded to ``get_gmail_service``.
    """
    if not email_ids:
        return

    service = get_gmail_service(creds=creds)

    # Find or create the label
    existing_labels = service.users().labels().list(userId="me").execute().get("labels", [])
    label_id: str | None = None
    for lbl in existing_labels:
        if lbl["name"] == "BarrioScout/Procesado":
            label_id = lbl["id"]
            break

    if label_id is None:
        created = service.users().labels().create(
            userId="me",
            body={
                "name": "BarrioScout/Procesado",
                "labelListVisibility": "labelShow",
                "messageListVisibility": "show",
            },
        ).execute()
        label_id = created["id"]
        print(f"Created Gmail label 'BarrioScout/Procesado' (id={label_id})")

    for msg_id in email_ids:
        service.users().messages().modify(
            userId="me",
            id=msg_id,
            body={"addLabelIds": [label_id], "removeLabelIds": ["INBOX", "UNREAD", "CATEGORY_PERSONAL"]},
        ).execute()

    print(f"Archived {len(email_ids)} emails with label 'BarrioScout/Procesado'")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the full Idealista email ingestion pipeline."""
    print("=== Idealista Email Ingestion ===")

    listings = extract()
    if not listings:
        print("No new listings found.")
        return

    print(f"\nExtracted {len(listings)} raw listings")

    df = transform(listings)

    # Summary
    print(f"\n--- Summary ---")
    print(f"Total listings: {len(df)}")
    if "operation_type" in df.columns:
        print(df["operation_type"].value_counts().to_string())
    if "city" in df.columns:
        print(df["city"].value_counts().to_string())

    load(df)

    email_ids = list({row["email_id"] for row in listings})
    post_process(email_ids)

    print("\nDone.")


if __name__ == "__main__":
    main()
