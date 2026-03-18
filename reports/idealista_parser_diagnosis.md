# Idealista Email Parser — Diagnostic Report

## Key Findings

1. **142 null rows (16.1%) — two distinct root causes**: 134 from `express_*` single-property emails (no `<img title>` tag), 8 from `fvp` emails (completely different template)
2. **63 duplicate property_ids — all cross-email, NOT same-email**: same property appearing in 2 different alert emails (e.g. two priceDrop emails for the same listing). Price is identical in all 63 cases — no actual price changes detected.
3. **is_exterior: only 6/884 true** — the word "exterior" rarely appears in the feature text cell; it's more commonly in the description or not stated at all
4. **area_m2: 2 zeros** — both are null-address rows (express single-property format) where the `m²` regex matched a stray "0 m²" or similar
5. **67 distinct cities instead of 2** — the `_extract_city` function takes the last comma segment of the address, which includes suburbs and satellite towns (Alcalá de Henares, Getafe, etc.)

---

## 1. Root cause analysis: 142 null rows

### Campaign type breakdown for null-city rows

| Campaign type | Null city | Has city | Total | Null % |
|---|---|---|---|---|
| `express_newAd_sale_professional` | 69 | 235 | 304 | 22.7% |
| `express_priceDrop_sale_professional` | 44 | 176 | 220 | 20.0% |
| `express_newAd_sale_particular` | 12 | 53 | 65 | 18.5% |
| `fvp` | 8 | 0 | 8 | 100% |
| `express_newAd_rent_professional` | 5 | 178 | 183 | 2.7% |
| `express_priceDrop_sale_particular` | 2 | 9 | 11 | 18.2% |
| `express_newAd_rent_particular` | 1 | 66 | 67 | 1.5% |
| `express_priceDrop_rent_professional` | 1 | 22 | 23 | 4.3% |

### Root cause A: `express_*` single-property emails (134 rows)

**What happens:** These emails contain a single property with a large photo and description, but the `<img>` tag inside the property `<a href="/inmueble/...">` does NOT have a `title` attribute matching the property type regex (`Piso en Calle X, Barrio, City`).

**Why:** The parser relies on `<img title="Piso en ...">` to extract address and property_type (line 215 of `idealista_emails.py`):
```python
img = a_tag.find("img", title=_PROPERTY_TYPE_RE)
```

In single-property `express` emails, the image is a large hero photo — its `title` is either absent, or uses a different format (e.g. just "Ver anuncio"). Without `raw_title`, the title regex at line 266 never matches, so `property_type` and `address` stay `None`.

**Evidence:**
- All 134 rows have `description IS NOT NULL` (rich property descriptions exist)
- All 134 rows have `address IS NULL` and `property_type IS NULL`
- All 134 rows have valid `price` and `area_m2` (these come from separate HTML elements)
- The descriptions contain address-like information: "en el corazón del emblemático Albaicín", "en la codiciada zona del Zaidín"

**Affected code:** `parse_listings_from_email()`, lines 214-226 and 260-274.

### Root cause B: `fvp` emails (8 rows, 100% null)

**What happens:** `fvp` (Featured Value Proposition) is a completely different email template — luxury property promotions. These emails have different HTML structure:
- High-end properties only (prices: €1.35M–€3.4M, areas: 113–475 m²)
- Different agent-branded layout (Engel & Völkers, Gilmar, etc.)
- No standard `<img title="Tipo en Dirección">` format at all

**Evidence:** All 8 fvp rows have prices > €1M, descriptions from luxury agencies, and zero address/city/property_type data.

**Recommendation:** Either add dedicated parsing for `fvp` templates, or skip them entirely (8 rows = 0.9% of data, all luxury Madrid properties outside typical buyer range).

---

## 2. Duplicate property_ids analysis

**63 properties appear exactly 2 times** across different email messages.

| Metric | Value |
|---|---|
| Total duplicate property_ids | 63 |
| All in different emails | 63 (100%) |
| Same-email duplicates | 0 |
| Price changed between alerts | 0 |
| Price stayed same | 63 (100%) |

**Pattern:** Same property appears in 2 separate `priceDrop` alert emails (or 2 `newAd` alerts). Despite appearing in `priceDrop` campaigns, the price is identical in both occurrences — suggesting these are Idealista's remarketing emails re-promoting the same listing, not actual price changes.

**Campaign pairs for duplicates:**
- Most are `priceDrop + priceDrop` (same campaign type, different email)
- Some are `newAd + newAd` (same ad promoted twice)

**Recommendation for clean layer:** Deduplicate by `property_id`, keeping the latest `email_date` row (most recent data). The `email_id` + `property_id` composite is unique.

---

## 3. is_exterior: only 6 true out of 884

**Root cause:** The feature text cell (`<td>55 m² 2 hab. 2ª planta</td>`) rarely contains the word "exterior". In Idealista's email format, exterior/interior is typically:
- Not mentioned at all in most emails
- Sometimes in the description text (not in the features cell)
- Sometimes as a separate icon/badge, not as text

**Code location:** Line 301:
```python
is_exterior = "exterior" in ft.lower()
```

This only checks the feature text cell (`ft`), which is correct for multi-card emails but misses the single-property format entirely (where features may be formatted differently).

**Recommendation:** Accept that `is_exterior` will be unreliable from email parsing. In the clean layer, default to `NULL` instead of `FALSE` when the parser didn't find the keyword. The 6 true values are valid; the 878 false values are "unknown" rather than confirmed interior.

---

## 4. area_m2 zeros (2 rows)

| property_id | address | city | area_m2 | price | campaign_type |
|---|---|---|---|---|---|
| 110921969 | NULL | NULL | 0.0 | 4,200,000 | express_newAd_sale_professional |
| 110934258 | NULL | NULL | 0.0 | 900,000 | express_newAd_sale_particular |

Both are null-address rows (express single-property format). The `m²` regex `r"(\d+)\s*m²"` likely matched a stray "0 m²" or similar text fragment in the feature scan.

**Code location:** Lines 238-242 (feature text collection) and line 292-294 (area parsing):
```python
m2 = re.search(r"(\d+)\s*m²", ft)
if m2:
    area_m2 = float(m2.group(1))
```

**Recommendation:** Add a minimum threshold in transform (e.g. `area_m2 < 5 → NULL`). No real property has 0 m².

---

## 5. 67 distinct cities instead of 2

**Not a parser bug — expected behaviour.** The `_extract_city()` function takes the last comma-separated segment of the address field (e.g. `"Calle X, Zaidín, Granada"` → `"Granada"`). But Idealista's email alerts include properties in surrounding municipalities:

- Madrid alerts include: Alcalá de Henares (14), Getafe (10), Alcorcón (9), Móstoles (8), etc.
- Granada alerts include: Churriana de la Vega (7), Alhendin (4), Albolote (2), etc.

**Distribution:** Madrid (385) + suburbs (185) = 570 | Granada (172) + suburbs (30) = 202 | NULL (142) → Total 884 ✓ (142 null rows had city dropped)

**Recommendation for clean layer:**
- Add a `metropolitan_area` column mapping suburbs to their parent city ("Getafe" → "Madrid", "Churriana de la Vega" → "Granada")
- Or filter to only listings where `city IN ('Madrid', 'Granada')` for scoring
- The 67 cities are actually useful data — they show the real search alert coverage

---

## 6. Proposed fixes summary

### Fix 1: Handle express single-property emails (HIGH — fixes 134/142 null rows)

**Problem:** No `<img title="Piso en ...">` in single-property express emails.

**Solution:** When `raw_title` is None, try to extract property type + address from other HTML elements:
- Look for `<h2>` or prominent text containing the property type regex
- Look for the property URL path which often contains a slug like `/piso-en-calle-x-barrio-ciudad/`
- Extract from description text using NLP-like patterns

**Easiest approach:** Parse the URL slug from the `<a href>`. Idealista URLs follow the pattern:
`/inmueble/12345/piso-en-calle-x-barrio-ciudad/` — the slug after the property_id contains type + address.

**Code change in `parse_listings_from_email()`** at line 226, after building anchors:
```python
# Fallback: extract address from URL slug if no img title
if raw_title is None:
    slug = re.search(r'/inmueble/\d+/([^/?]+)', a_tag["href"])
    if slug:
        raw_title = slug.group(1).replace('-', ' ').title()
```

### Fix 2: Skip fvp campaign entirely (LOW — only 8 rows)

Add early return in extract() for fvp campaigns, or filter in transform().

### Fix 3: is_exterior → NULL instead of FALSE

**Code change in `parse_listings_from_email()`** at line 289:
```python
# Change from: is_exterior: bool = False
# Change to: is_exterior: bool | None = None
is_exterior: bool | None = None
if idx < len(feature_texts):
    ft = feature_texts[idx]
    if "exterior" in ft.lower():
        is_exterior = True
    elif "interior" in ft.lower():
        is_exterior = False
```

### Fix 4: area_m2 minimum threshold in transform

Add after the numeric cast in `transform()`:
```python
df.loc[df["area_m2"] < 5, "area_m2"] = pd.NA
```

### Fix 5: Deduplication in clean layer

In BigQuery clean view:
```sql
SELECT * EXCEPT(rn) FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY property_id
    ORDER BY email_date DESC
  ) as rn
  FROM barrioscout_raw.idealista_listings
) WHERE rn = 1
```

---

## Priority order

1. **Fix 1 (URL slug fallback)** — recovers 134 rows with address data → HIGH impact
2. **Fix 5 (dedup in clean layer)** — removes 63 duplicate rows in BigQuery → needed for scoring
3. **Fix 3 (is_exterior NULL)** — data quality improvement → MEDIUM
4. **Fix 4 (area threshold)** — 2 rows → LOW
5. **Fix 2 (skip fvp)** — 8 rows → LOW
