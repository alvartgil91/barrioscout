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

## Fase 2 — Pipelines de ingesta (en curso)

### INE Atlas de Distribución de Renta — descubrimiento de tablas por provincia
- **Problema**: La tabla 30896 configurada en Fase 1 solo cubría Cataluña (provincia 08), no España completa.
- **Investigación**: El INE publica una tabla por provincia dentro de la operación 353 (Atlas ADRH). No existe una tabla nacional única. Los IDs no siguen un offset fijo — se descubrieron probando la API `servicios.ine.es/wstempus/js/ES/TABLAS_OPERACION/353` y descargando muestras.
- **Decisión**: Configurar `INE_RENTA_TABLE_IDS: dict[str, int]` por ciudad objetivo en `config/settings.py`. Añadir una ciudad nueva = añadir una entrada al dict.
- **IDs correctos**: Granada (prov 18) = 31025, Madrid (prov 28) = 31097.
- **Pipeline**: `extract()` descarga y concatena ambas tablas en cada ejecución.

### INE IPV — cobertura y granularidad
- **Datos**: Trimestrales por CCAA (tabla 25171). No existe granularidad municipal.
- **Filtrado**: Andalucía (proxy Granada) y Comunidad de Madrid. Indicador: General (excluye nueva/segunda mano).
- **Uso**: Tendencia de precios a nivel regional; en Fase 3 se usará para ajustar estimaciones de renta reciente.

### INE Renta — cobertura temporal y gap 2024-2026
- **Datos disponibles**: 2015-2023 (publicación anual en octubre).
- **Gap aceptable**: La renta municipal cambia lentamente; un gap de 2-3 años tiene impacto mínimo. Dashboard mostrará "Renta neta media (2023)".
- **Mejora futura (Fase 3)**: Ajustar con variación IPV regional para obtener una estimación más reciente.

---

## Fases planificadas

| Fase | Descripción | Sesiones estimadas |
|------|-------------|-------------------|
| F1 | Setup + validación fuentes | COMPLETADA |
| F2 | Pipelines de ingesta | 3-4 sesiones |
| F3 | Scoring engine (ubicación + precio) | 2-3 sesiones |
| F4 | Streamlit dashboard + recomendador | 3-4 sesiones |
| F5 | AI insights precalculados (Claude API) | 1-2 sesiones |
