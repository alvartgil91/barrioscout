"""Fetch 2 sample emails per campaign type and save raw HTML for field audit."""

from __future__ import annotations

import base64
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bs4 import BeautifulSoup
from src.ingestion.idealista_emails import get_gmail_service, _decode_body

CAMPAIGN_TYPES = [
    "express_newAd_sale_professional",
    "express_priceDrop_sale_professional",
    "express_newAd_rent_professional",
    "express_newAd_rent_particular",
    "express_newAd_sale_particular",
    "express_priceDrop_rent_professional",
    "express_priceDrop_sale_particular",
    "fvp",
    "express_priceDrop_rent_particular",
]

SENDERS = ["alertas@idealista.com", "no-reply@idealista.com", "noresponder@idealista.com"]
OUT_DIR = "reports/email_samples"


def main() -> None:
    service = get_gmail_service()
    sender_query = " OR ".join(f"from:{s}" for s in SENDERS)
    base_query = f"in:anywhere ({sender_query})"

    # Fetch ALL processed emails (they have the label)
    query = f"{base_query} label:BarrioScout/Procesado"
    print(f"Searching: {query}")
    response = service.users().messages().list(userId="me", q=query, maxResults=500).execute()
    messages = response.get("messages", [])
    print(f"Found {len(messages)} processed emails total")

    # Fetch each email, classify by campaign type, save up to 2 per type
    saved: dict[str, int] = {ct: 0 for ct in CAMPAIGN_TYPES}
    saved["unknown"] = 0

    for i, msg_meta in enumerate(messages):
        # Stop if we have 2 of each
        if all(v >= 2 for v in saved.values() if True):
            needed = [ct for ct, v in saved.items() if v < 2 and ct != "unknown"]
            if not needed:
                break

        msg_id = msg_meta["id"]
        msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()

        html_body = _decode_body(msg["payload"])
        if not html_body:
            continue

        soup = BeautifulSoup(html_body, "lxml")

        # Detect campaign type
        campaign = None
        for a_tag in soup.find_all("a", href=True):
            m = re.search(r"utm_campaign=([^&\"'\s]+)", a_tag["href"])
            if m:
                campaign = m.group(1)
                break

        if campaign is None:
            campaign = "unknown"

        if campaign not in saved:
            campaign_key = "unknown"
        else:
            campaign_key = campaign

        if saved.get(campaign_key, 0) >= 2:
            continue

        saved[campaign_key] = saved.get(campaign_key, 0) + 1
        idx = saved[campaign_key]

        # Save HTML
        filename = f"{campaign_key}_{idx}.html"
        filepath = os.path.join(OUT_DIR, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html_body)

        print(f"  [{i+1}] Saved {filename} (msg_id={msg_id[:12]}..)")

        if i > 200:  # Safety limit
            break

    print(f"\nSaved samples: {saved}")
    print("Done.")


if __name__ == "__main__":
    main()
