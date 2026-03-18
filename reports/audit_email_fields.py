"""Analyze HTML structure of each email campaign type to inventory all extractable fields."""

from __future__ import annotations

import os
import re
from bs4 import BeautifulSoup

OUT_DIR = "reports/email_samples"


def analyze_email(filepath: str) -> dict:
    """Extract every possible field from an email HTML."""
    with open(filepath, "r", encoding="utf-8") as f:
        html = f.read()

    soup = BeautifulSoup(html, "lxml")
    result: dict = {"file": os.path.basename(filepath)}

    # 1. Property links and URL slugs
    prop_links = []
    for a_tag in soup.find_all("a", href=True):
        m = re.search(r"/inmueble/(\d+)/([^?\"']*)", a_tag["href"])
        if m:
            prop_id = m.group(1)
            slug = m.group(2).rstrip("/")
            prop_links.append({"property_id": prop_id, "slug": slug, "full_url": a_tag["href"].split("?")[0]})
    result["property_links"] = prop_links[:5]  # Limit

    # 2. Image titles
    img_titles = []
    for img in soup.find_all("img", title=True):
        title = img.get("title", "").strip()
        if title and len(title) > 5:
            img_titles.append(title)
    result["img_titles"] = img_titles[:10]

    # 3. Price-like texts (€ symbol)
    price_texts = []
    for tag in soup.find_all(["span", "td", "div", "p", "strong", "b"]):
        text = tag.get_text(" ").strip()
        if "€" in text and len(text) < 80 and re.search(r"\d", text):
            # Avoid duplicating parent/child text
            if text not in price_texts:
                price_texts.append(text)
    result["price_texts"] = price_texts[:15]

    # 4. Previous price / discount (priceDrop specific)
    discount_texts = []
    for tag in soup.find_all(["span", "td", "div", "p", "strong"]):
        text = tag.get_text(" ").strip()
        if any(x in text for x in ["↓", "bajada", "antes", "rebaja", "%"]) and len(text) < 100:
            if text not in discount_texts:
                discount_texts.append(text)
    result["discount_texts"] = discount_texts[:10]

    # 4b. Strikethrough prices (old price)
    strikethrough = []
    for tag in soup.find_all(["s", "del", "strike"]):
        text = tag.get_text(" ").strip()
        if text:
            strikethrough.append(text)
    # Also check for text-decoration: line-through
    for tag in soup.find_all(style=re.compile(r"line-through")):
        text = tag.get_text(" ").strip()
        if text and text not in strikethrough:
            strikethrough.append(text)
    result["strikethrough_prices"] = strikethrough[:5]

    # 5. Feature texts (m², hab, planta, etc.)
    feature_texts = []
    for tag in soup.find_all(["td", "div", "span", "p"]):
        text = tag.get_text(" ").strip()
        if ("m²" in text or "hab" in text or "planta" in text or "baño" in text) and len(text) < 200:
            if text not in feature_texts:
                feature_texts.append(text)
    result["feature_texts"] = feature_texts[:15]

    # 6. Elevator, garage, exterior mentions
    all_text = soup.get_text(" ", strip=True).lower()
    result["mentions"] = {
        "ascensor": "ascensor" in all_text,
        "garaje": "garaje" in all_text or "parking" in all_text or "plaza de garaje" in all_text,
        "exterior": "exterior" in all_text,
        "interior": "interior" in all_text,
        "trastero": "trastero" in all_text,
        "terraza": "terraza" in all_text,
        "piscina": "piscina" in all_text,
        "reformar": "reformar" in all_text or "a reformar" in all_text,
        "buen estado": "buen estado" in all_text,
        "nuevo": "obra nueva" in all_text or "a estrenar" in all_text,
        "certificado": "certificado" in all_text or "energétic" in all_text,
        "baño": "baño" in all_text or "aseo" in all_text,
    }

    # 7. Specific feature badges/icons (common in express emails)
    badges = []
    for tag in soup.find_all(["td", "span", "div"]):
        text = tag.get_text(" ").strip()
        if re.match(r"^\d+\s*(baño|aseo)", text):
            badges.append(f"bathrooms: {text}")
        if re.match(r"^(con|sin)\s+ascensor", text, re.I):
            badges.append(f"elevator: {text}")
        if re.match(r"^(exterior|interior)$", text, re.I):
            badges.append(f"orientation: {text}")
    result["badges"] = badges

    # 8. UTM campaign
    for a_tag in soup.find_all("a", href=True):
        m = re.search(r"utm_campaign=([^&\"'\s]+)", a_tag["href"])
        if m:
            result["campaign"] = m.group(1)
            break

    # 9. Agent/professional info
    agent_texts = []
    for tag in soup.find_all(["span", "td", "div", "p"]):
        text = tag.get_text(" ").strip()
        if any(x in text.lower() for x in ["inmobiliaria", "agencia", "contactar", "anunciante"]):
            if len(text) < 100 and text not in agent_texts:
                agent_texts.append(text)
    result["agent_texts"] = agent_texts[:5]

    # 10. All text blocks > 20 chars (to find anything we missed)
    all_blocks = []
    for tag in soup.find_all(["td", "span", "div", "p", "h1", "h2", "h3", "strong", "b"]):
        text = tag.get_text(" ").strip()
        if 20 < len(text) < 200 and text not in all_blocks:
            all_blocks.append(text)
    result["text_blocks_sample"] = all_blocks[:30]

    return result


def main() -> None:
    files = sorted(f for f in os.listdir(OUT_DIR) if f.endswith(".html"))
    print(f"Analyzing {len(files)} email samples...\n")

    for f in files:
        filepath = os.path.join(OUT_DIR, f)
        result = analyze_email(filepath)

        print(f"={'='*70}")
        print(f"FILE: {f}")
        print(f"CAMPAIGN: {result.get('campaign', '?')}")
        print(f"={'='*70}")

        print(f"\nPROPERTY LINKS ({len(result['property_links'])}):")
        for pl in result["property_links"]:
            print(f"  id={pl['property_id']} slug='{pl['slug']}' url={pl['full_url'][:100]}")

        print(f"\nIMG TITLES ({len(result['img_titles'])}):")
        for t in result["img_titles"]:
            print(f"  '{t}'")

        print(f"\nPRICE TEXTS ({len(result['price_texts'])}):")
        for t in result["price_texts"]:
            print(f"  '{t}'")

        print(f"\nDISCOUNT/PREVIOUS PRICE:")
        for t in result["discount_texts"]:
            print(f"  '{t}'")
        for t in result["strikethrough_prices"]:
            print(f"  [strikethrough] '{t}'")

        print(f"\nFEATURE TEXTS ({len(result['feature_texts'])}):")
        for t in result["feature_texts"]:
            print(f"  '{t}'")

        print(f"\nBADGES: {result['badges']}")
        print(f"MENTIONS: {result['mentions']}")
        print(f"\nAGENT TEXTS: {result['agent_texts']}")

        print(f"\nTEXT BLOCKS (sample):")
        for t in result["text_blocks_sample"][:15]:
            print(f"  '{t[:100]}'")

        print()


if __name__ == "__main__":
    main()
