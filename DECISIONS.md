# BarrioScout — Decision Log

Registro de decisiones técnicas y de producto tomadas durante el desarrollo.
Este archivo sirve como documentación para el blog y referencia futura.

---

## Fase 0 — Diseño y planificación

### Nombre del proyecto
- **Decisión**: BarrioScout ("barrio" local español + "scout" explorar oportunidades)
- **Alternativas descartadas**: SpainRE Intelligence, HabiScore, CasaData, InmoScope, UrbanRadar, CasaLens, PropScore, NestMetrics, BarrioLens

### Stack tecnológico
- **Dashboard**: Streamlit (Python puro, 0€, rápido de construir)
- **Descartados**: Evidence.dev (curva nueva innecesaria), Next.js (demasiado trabajo frontend para un proyecto de datos)
- **AI**: Precalculado con Claude API, batch mensual guardado en BQ. 0€ por visita.
- **Descartado**: AI en tiempo real (coste por visita incontrolable en app pública)

### Ingesta de datos
- **Decisión**: Python puro (requests + pandas + google-cloud-bigquery)
- **Descartados**: dlt (sobreingeniería para 5 fuentes), Mage/Airflow/Prefect (necesitan servidor 24/7, overkill para actualización trimestral)
- **Razón**: Para portfolio, Python puro demuestra que sabes construir pipelines desde cero

### Patrón de datos
- **Decisión**: Medallion (raw → clean → analytics) en BigQuery
- **raw**: dato tal cual llega, append-only
- **clean**: tipado, deduplicado, normalizado (SQL views en BQ)
- **analytics**: agregaciones que consume Streamlit (SQL views en BQ)

### Datos de portales inmobiliarios
- **Idealista API**: solicitada hace ~10 años, credenciales perdidas, límites muy cortos
- **Scraping continuo**: descartado (anti-bot, ToS, mala imagen en portfolio)
- **Decisión**: Carga inicial scrapeando una vez + alertas email de Idealista/Fotocasa para nuevos pisos
- **Flujo**: Emails → parseo con Python/n8n → BigQuery
- **MCP scrapers (Bright Data, Apify)**: descartados por coste mensual, objetivo es 0€

### Infraestructura
- **MacBook 2013 como servidor**: descartado (30-60W = 15-25€/mes electricidad > VPS Hetzner 5€/mes)
- **VPS**: pendiente, no necesario hasta tener pipelines automatizados
- **Coste objetivo**: 0€/mes durante desarrollo, máximo 5€/mes en producción

### Alcance geográfico
- **Decisión**: Granada + Madrid (dos mercados diferentes: pequeño/turístico vs grande/competitivo)
- **Razón**: Demuestra que el sistema escala. "¿Y Barcelona?" → "Solo hay que añadir la ciudad al config"

---

## Fase 1 — Setup + validación de fuentes

- **Fecha**: marzo 2025
- **Resultado**: COMPLETADA ✅
- **Archivos creados**: 21
- **Tests de validación**:
  - INE IPV (precio vivienda): OK — 18.240 rows
  - Catastro INSPIRE WFS: OK — XML válido
  - OpenStreetMap Overpass: OK — 12 hospitales en Granada
  - Google Places: SKIP — sin API key configurada
  - INE Renta media: OK — datos cargados
- **Fix detectado**: INE usa separador ";" no "\t" — a corregir en Fase 2
- **Commits**: 2 (estructura inicial + fix source precio vivienda)

---

## Fase 2 — Pipelines de ingesta (completada)

### INE Atlas de Distribución de Renta — descubrimiento de tablas por provincia
- **Problema**: La tabla 30896 configurada en Fase 1 solo cubría Cataluña (provincia 08), no España completa.
- **Investigación**: El INE publica una tabla por provincia dentro de la operación 353 (Atlas ADRH). No existe una tabla nacional única. Los IDs no siguen un offset fijo — se descubrieron probando la API `servicios.ine.es/wstempus/js/ES/TABLAS_OPERACION/353` y descargando muestras.
- **Decisión**: Configurar `INE_RENTA_TABLE_IDS: dict[str, int]` por ciudad objetivo en `config/settings.py`. Añadir una ciudad nueva = añadir una entrada al dict.
- **IDs correctos**: Granada (prov 18) = 31025, Madrid (prov 28) = 31097.
- **Pipeline**: `extract()` descarga y concatena ambas tablas en cada ejecución.

### Catastro INSPIRE WFS (Buildings)
- **Endpoint**: `wfsBU.aspx` (buildings). El código anterior usaba `wfsCP.aspx` (parcelas) — corregido.
- **Límite API**: El límite real es ~1km² (la documentación indica 4km² pero está desactualizada; probado: 1000m OK, 1100m falla). Solución: tiling con pyproj (EPSG:4326→25830). Tile size 900m (~0.81km²). Sleep 1s entre requests por cortesía.
- **Datos extraídos**: ref catastral, año construcción, uso actual, centroide WGS84 (derivado del midpoint del bbox EPSG:25830).
- **Namespaces reales**: `bu-ext2d`/`bu-core2d` en `inspire.jrc.ec.europa.eu/schemas` (no `inspire.ec.europa.eu`).
- **Encoding respuesta**: ISO-8859-1.

### INE IPV — cobertura y granularidad
- **Datos**: Trimestrales por CCAA (tabla 25171). No existe granularidad municipal.
- **Filtrado**: Andalucía (proxy Granada) y Comunidad de Madrid. Indicador: General (excluye nueva/segunda mano).
- **Uso**: Tendencia de precios a nivel regional; en Fase 3 se usará para ajustar estimaciones de renta reciente.

### INE Renta — cobertura temporal y gap 2024-2026
- **Datos disponibles**: 2015-2023 (publicación anual en octubre).
- **Gap aceptable**: La renta municipal cambia lentamente; un gap de 2-3 años tiene impacto mínimo. Dashboard mostrará "Renta neta media (2023)".
- **Mejora futura (Fase 3)**: Ajustar con variación IPV regional para obtener una estimación más reciente.

### OSM POIs — cobertura y rate limits
- **Query Overpass**: node+way+relation con amenity+shop keys. Transport incluye extras: railway=subway_entrance, railway=station, railway=tram_stop, public_transport=station.
- **out center**: para obtener lat/lon de ways y relations (no tienen coordenadas top-level como los nodes).
- **Dedup**: por osm_id — un POI mapeado como node y way solo cuenta una vez.
- **Rate limits**: Madrid education (504) y shopping (429) fallaron en primera ejecución. Retry manual 2 min después funcionó. Sleep 2s entre requests insuficiente para bboxes grandes.
- **Resultado**: Granada 714 POIs (200 edu + 321 health + 36 transport + 157 shopping). Madrid 7,346 POIs (2472 edu + 2717 health + 1014 transport + 1143 shopping). Total: 8,060 unique POIs.

### Catastro — timeouts y cobertura parcial
- **Granada**: 154 tiles, ~40 timeouts (60s cada uno), 12,180 buildings cargados. Cobertura ~80% estimada.
- **Madrid**: 930 tiles, ejecución cancelada tras >1h. Pendiente ejecutar en background.
- **Decisión**: Añadir retry logic con reintentos por tile fallido + flag --city para ejecutar una sola ciudad sin duplicar datos.

### Carga raw layer — estado
- BigQuery API habilitada, dataset barrioscout_raw creado.
- INE Renta: 3,120 rows. INE IPV: 608 rows. Catastro: 12,180 (solo Granada). OSM POIs: 8,060. Total: ~24,000 rows.
- La deduplicación definitiva se hará en barrioscout_clean (Fase 3).

### Ministerio de Transportes — datos notariales y valor tasado (Fase 2.5)
- **Fuente**: Boletín estadístico online del Ministerio (apps.fomento.gob.es/BoletinOnline2/)
- **WAF**: El dominio principal transportes.gob.es bloquea scripts, pero apps.fomento.gob.es permite descarga directa de XLS.
- **Decisión**: Descarga manual de dos XLS a data/raw/, pipeline lee desde disco local (no HTTP).
- **Transacciones por municipio**: 1 sheet wide, 88 trimestres (2004-Q1 → 2025-Q4), ~8,171 municipios de toda España. Se filtran solo Granada y Madrid. Unpivot a formato long.
- **Valor tasado €/m²**: 84 sheets (1 por trimestre), solo municipios >25K hab. "n.r" = no reportado → NaN. Forward-fill en provincia (celdas mergeadas).
- **Cobertura**: Datos trimestrales 2004-2025 (transacciones), 2005-2025 (valor tasado). Ambos incluyen Granada y Madrid.
- **Valor para el proyecto**: Precio oficial de referencia por municipio + volumen de mercado. Complementa INE IPV (que es por CCAA) con granularidad municipal.

### Fase 2 — Resumen final
- **Estado**: COMPLETADA ✅
- **Raw layer en BigQuery** (dataset: barrioscout_raw, proyecto: portfolio-alvartgil91):
  - ine_renta: 3,120 rows — renta media por municipio (2015-2023, Granada+Madrid)
  - ine_ipv: 608 rows — índice precios vivienda trimestral por CCAA (2007-2025)
  - catastro_buildings: 72,684 rows — edificios (12,180 Granada + 60,504 Madrid)
  - osm_pois: 8,060 rows — POIs de 4 categorías (education, health, transport, shopping)
  - ministerio_transacciones: 176 rows — compraventas trimestrales por municipio (2004-2025)
  - ministerio_valor_tasado: 168 rows — valor tasado €/m² trimestral por municipio (2005-2025)
  - Total: ~84,816 rows
- **Hallazgos técnicos clave**:
  - INE publica una tabla de renta por provincia, no nacional. IDs descubiertos probando la API.
  - Catastro INSPIRE tiene límite real de ~1km² (no 4km² como dice la doc). Tiles de 900m. Retry logic necesaria (timeouts frecuentes).
  - Overpass rate limits para bboxes grandes (Madrid). Sleep 2s insuficiente; retry manual necesario.
  - Ministerio WAF bloquea descargas con scripts; XLS descargados manualmente. Formato complejo con merged cells.
  - Python buffering: necesario -u flag para logs en background (nohup).
- **Archivos de ingesta**: ine.py, ine_ipv.py, catastro.py, osm_pois.py, ministerio_transacciones.py, ministerio_valor_tasado.py
- **Nota**: ministerio.py (Fase 1) queda como legacy; los nuevos pipelines lo reemplazan.

---

## Fase 2.6b — Automatización: Cloud Function para Idealista emails

### Decisión: Cloud Function 2nd gen + Cloud Scheduler (cada 6h)
- **Motivación**: El pipeline de Idealista emails (`idealista_emails.py`) funcionaba solo con ejecución manual local. Con ~50 emails/día, necesita automatización para no perder datos.
- **Alternativas descartadas**:
  - Cron local / VPS: requiere máquina encendida 24/7 (coste electricidad o VPS)
  - Cloud Run Job: viable pero más complejo de configurar que Cloud Function para un trigger simple
  - Workflow / Composer: sobreingeniería para un solo pipeline

### Decisión: Desplegar desde raíz del repo (--source=.)
- **Motivación**: Evitar duplicar código de `src/` y `config/` en un subdirectorio separado.
- **Mecanismo**: `main.py` en la raíz como entry point. Cloud Functions empaqueta todo el directorio. El deploy script intercambia temporalmente `requirements.txt` con `cf_requirements.txt` y lo restaura al terminar.
- **Trade-off**: Se sube más código del necesario al Cloud Function (otros módulos de ingesta, notebooks, etc.), pero el bundle sigue siendo pequeño (<5MB) y simplifica enormemente el mantenimiento.

### Decisión: OAuth token en Secret Manager con refresh automático
- **Flujo**: Token almacenado en `gmail-oauth-token`. Si expira, la Cloud Function lo refresca con `creds.refresh()` y escribe una nueva versión del secreto.
- **Riesgo**: Apps OAuth en modo "testing" revocan refresh tokens tras 7 días. La app debe estar en modo "production" o "internal" (Workspace).
- **Fallback**: Si el token no se puede refrescar, la función falla con un error claro indicando que hay que regenerar localmente.

### Decisión: max_emails=50 por ejecución
- **Motivación**: Con ~50 emails × 3 intentos geocoding × 1.1s = hasta 165s por batch. Mantiene la ejecución dentro del timeout de 540s incluso con backlog.
- **Mecanismo**: `extract(max_emails=50)`. Los emails no procesados se recogen en la siguiente ejecución (cada 6h).

### Decisión: Service account dedicada (barrioscout-cf)
- **Roles**: secretmanager.secretAccessor, secretmanager.secretVersionAdder, bigquery.dataEditor, bigquery.jobUser.
- **Principio de mínimo privilegio**: Sin acceso a otros recursos del proyecto.

### Decisión: Zero breaking changes en pipeline local
- **`get_gmail_service(creds=None)`**: Si no se pasan credenciales, funciona igual que antes (archivos locales + browser flow).
- **`extract(max_emails=200)`**: El default de 200 mantiene el comportamiento original para ejecución local.
- **`config/settings.py`**: `GMAIL_TOKEN_PATH` y `GMAIL_CREDENTIALS_PATH` leen de env vars con fallback al valor original.

---

## Fase 2.7 — Polígonos de barrios y distritos

### Fuentes elegidas
- **Madrid**: TopoJSON oficial del Ayuntamiento (geoportal.madrid.es). Barrios (131 polígonos) y Distritos (21 polígonos) como archivos separados. Coordenadas cuantizadas en WGS84. Decodificación manual sin dependencia `topojson` — solo delta-decode + dequantize (~30 líneas).
- **Granada**: IDE Andalucía WFS DEA100, capa `dea100:da04_barrio`. GeoJSON directo (`OUTPUTFORMAT=application/json`). CRS nativo EPSG:23030 (ED50 / UTM 30N) — reproyección a WGS84 con pyproj.

### Decisión: distritos Granada por dissolve
- **Problema**: No existe capa de distritos como polígonos separados en el WFS. La capa `da06` es secciones censales, no distritos. El campo `distrito` existe como texto en cada barrio.
- **Solución**: `shapely.ops.unary_union` de barrios agrupados por campo `distrito` → genera 8 polígonos de distrito.
- **Alternativa descartada**: Overpass API (`admin_level=9`) — datos OSM menos oficiales y cobertura incierta para Granada.

### Decisión: WKT en raw, GEOGRAPHY en clean
- **Motivación**: BigQuery GEOGRAPHY no permite `autodetect=True` en load jobs. Almacenar como STRING (WKT) en raw, convertir con `ST_GEOGFROMTEXT(geometry_wkt)` en vistas clean/analytics.
- **Ventaja**: Consistente con el patrón raw = dato tal cual llega, minimal transformation.

### Hallazgos del probing
- **Madrid TopoJSON**: Coordenadas ya en WGS84 (lon/lat), no necesita reproyección. 131 barrios todos Polygon (ningún MultiPolygon). Properties útiles: NOMBRE, NOMDIS (distrito padre), COD_BAR.
- **Granada WFS**: 37 barrios en 8 distritos. 2 nombres duplicados entre distritos distintos ("Joaquina Eguaras" en Beiro y Norte, "San Matías-Realejo" 2× en Centro). Se resuelve usando distrito+barrio como identificador.
- **CRS Granada**: EPSG:23030 (metros), magnitud x~449000 y~4112000. Reproyección obligatoria.

### Datos cargados
- Madrid: 131 barrios + 21 distritos = 152 rows
- Granada: 37 barrios + 8 distritos (dissolved) = 45 rows
- Total: ~197 rows → barrioscout_raw.neighborhoods

---

## Fase 2.8 — Geocoding fixes + Polígonos metropolitanos + Two-pass spatial join (2026-03-23)

### Geocoding fix
- **Problema**: 4 listings geocodificados fuera del área correcta (ej. Cerceda en Galicia en lugar de Madrid) + 2 Gran Vía en Motril en lugar de Madrid/Granada.
- **Causa raíz**: Emails sin coma en la dirección → `city` field = dirección completa → Google Maps sin contexto geográfico.
- **Solución**: Extraer `alert_city` del href "Ver todos los anuncios" del email → `components` bias en Google Maps API → bbox validation con retry.
- **Fallback**: `geocode_level = "UNVERIFIED"` para coordenadas fuera del bbox de la ciudad. No se pierde ningún listing.
- **6 listings corregidos manualmente**: 4 bad geocodes Madrid + 2 Gran Vía Motril.

### Polígonos municipales metropolitanos
- **Decisión**: municipio = barrio (no buscar subdivisiones internas de municipios). Un polígono por municipio.
- **Fuente**: OSM Overpass API, `admin_level=8` (nivel municipal en España). Script `scripts/download_municipal_polygons.py`.
- **Cobertura**: 52 municipios (44 primera ronda + 7 segunda ronda + Villa de Otura). Cargados en `barrioscout_raw.neighborhoods` con `code LIKE 'metro_%'`.
- **Nomenclatura OSM**: Algunos municipios tienen nombre oficial diferente al usado en Idealista (ej. "Otura" en Idealista → "Villa de Otura" en OSM). Descubierto con bbox query.
- **metro_area como campo derivado**: No se almacena en raw. Se deriva en `dim_neighborhoods` con `ST_Y(ST_CENTROID(geometry)) > 39.0 → 'Madrid', else 'Granada'`. Robusto para cualquier municipio nuevo añadido sin necesidad de mapeado manual.
- **city = nombre del municipio**: No se fuerza a "Madrid" o "Granada" para no falsear la jerarquía de barrios/distritos de la ciudad.
- **Walkability metro = 0**: Aceptado como limitación actual. Los municipios metropolitanos tienen `health_count = education_count = shopping_count = transport_count = 0` porque los POIs de OSM solo se ingirieron para los bboxes de las ciudades principales. Pendiente: ingestar POIs para municipios metro.

### Two-pass spatial join (`fct_listing_observations`)
- **Problema**: ~176 orphan listings cuyos geocodes caían en gaps entre polígonos de barrios (bordes, carreteras).
- **Solución**: Two-pass join: (1) `ST_WITHIN` exacto, (2) `ST_DWITHIN(200m)` nearest-neighbor para los que no matchean exactamente.
- **Implementación**: `exact_match` CTE con `LEFT JOIN` + `nearest_fallback` CTE con `CROSS JOIN` filtrado y `QUALIFY ROW_NUMBER()`.
- **Resultado**: 1,249 → 1,723 assigned (65.1% → 92.9%). 131 orphans restantes corresponden a municipios por debajo del umbral de descarga (1–3 listings) y ~3 gaps estructurales en Granada ciudad.
- **Scope**: Solo se aplica a `fct_listing_observations`. Los otros spatial joins (`int_neighborhood_buildings`, `int_neighborhood_pois`) mantienen `ST_WITHIN` exacto — correcto porque buildings y POIs están siempre dentro de polígonos.

---

## Fases planificadas

| Fase | Descripción | Sesiones estimadas |
|------|-------------|-------------------|
| F1 | Setup + validación fuentes | COMPLETADA |
| F2 | Pipelines de ingesta | COMPLETADA (5 sesiones) |
| F3 | Scoring engine (ubicación + precio) | 2-3 sesiones |
| F4 | Streamlit dashboard + recomendador | 3-4 sesiones |
| F5 | AI insights precalculados (Claude API) | 1-2 sesiones |
