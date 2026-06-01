> ⚠️ HISTORICAL SNAPSHOT — generated ~March 2026. Row counts are stale. Current state documented in CLAUDE.md.

# BarrioScout — Raw Data Profiling Report

Dataset: `portfolio-alvartgil91.barrioscout_raw`

Tables: 8

Total rows: 85,897


---

## Executive Summary

**Dataset:** `portfolio-alvartgil91.barrioscout_raw` — 8 tables, 85,897 total rows

### Key findings

_(auto-generated — see detailed sections below for specifics)_


---


## Table Overview

| Table | Rows | Size |
|-------|------|------|
| `catastro_buildings` | 72,684 | 4464.4 KB |
| `idealista_listings` | 884 | 349.0 KB |
| `ine_ipv` | 608 | 38.4 KB |
| `ine_renta` | 3,120 | 164.0 KB |
| `ministerio_transacciones` | 176 | 7.0 KB |
| `ministerio_valor_tasado` | 168 | 9.3 KB |
| `neighborhoods` | 197 | 1241.1 KB |
| `osm_pois` | 8,060 | 663.5 KB |


## Per-table Profiles


### `catastro_buildings` (72,684 rows)

**Schema:**

| Column | Type | Mode |
|--------|------|------|
| `cadastral_ref` | STRING | NULLABLE |
| `year_built` | FLOAT | NULLABLE |
| `current_use` | STRING | NULLABLE |
| `latitude` | FLOAT | NULLABLE |
| `longitude` | FLOAT | NULLABLE |
| `_loaded_at` | TIMESTAMP | NULLABLE |

**Column profiles:**

**`cadastral_ref`** (STRING):
  - Distinct: 72,684, Nulls: 0 (0.0%), Empty strings: 0, Length: [14..14]
  - Top values:
    - `5106111VG4250E`: 1
    - `8620014VG4182B`: 1
    - `7845438VG4174F`: 1
    - `6925402VG4162F`: 1
    - `18900A00700241`: 1
    - `18900A00700014`: 1
    - `18900A01000135`: 1
    - `6470048VG4167A`: 1
    - `5846002VG4154F`: 1
    - `3171907VG4137A`: 1

**`year_built`** (FLOAT):
  - Min: 1,192, Max: 2,026, Mean: 1,983.97, Median: 1,985, Stddev: 23.86
  - Nulls: 180 (0.2%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 754

**`current_use`** (STRING):
  - Distinct: 6, Nulls: 256 (0.4%), Empty strings: 0, Length: [10..18]
  - Top values:
    - `1_residential`: 57,402
    - `3_industrial`: 7,649
    - `4_3_publicServices`: 3,021
    - `4_2_retail`: 1,735
    - `4_1_office`: 1,514
    - `2_agriculture`: 1,107

**`latitude`** (FLOAT):
  - Min: 37.12, Max: 40.58, Mean: 39.87, Median: 40.40, Stddev: 1.21
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 0

**`longitude`** (FLOAT):
  - Min: -3.86, Max: -3.51, Mean: -3.67, Median: -3.66, Stddev: 0.08
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 72,684
  - Outliers (>3σ): 0

**`_loaded_at`** (TIMESTAMP):
  - Range: 2026-03-15 19:49:42.738162+00:00 → 2026-03-16 13:18:14.205392+00:00
  - Nulls: 0 (0.0%), Future dates: 0

**Duplicates:** 0 full-row duplicates

**Null patterns:** `year_built`: 180 (0.2%) | `current_use`: 256 (0.4%)


#### Catastro Buildings — specific checks

**year_built:** range [1192.0..2026.0], pre-1800: 17, post-2025: 6, nulls: 180

**Year distribution:**

  - 1900-1949: 3,270
  - 1950-1979: 26,731
  - 1980-1999: 21,751
  - 2000-2009: 12,752
  - 2010+: 7,900
  - <1900: 100
  - NULL: 180

**current_use** (7 distinct values):

  - `1_residential`: 57,402
  - `3_industrial`: 7,649
  - `4_3_publicServices`: 3,021
  - `4_2_retail`: 1,735
  - `4_1_office`: 1,514
  - `2_agriculture`: 1,107
  - `None`: 256

**Coordinates:** 0 outside Spain bbox, 0 null coords

**Buildings by inferred city:**

  - Madrid: 60,504
  - Granada: 12,180


### `idealista_listings` (884 rows)

**Schema:**

| Column | Type | Mode |
|--------|------|------|
| `property_id` | STRING | NULLABLE |
| `operation_type` | STRING | NULLABLE |
| `property_type` | STRING | NULLABLE |
| `address` | STRING | NULLABLE |
| `city` | STRING | NULLABLE |
| `price` | FLOAT | NULLABLE |
| `area_m2` | FLOAT | NULLABLE |
| `bedrooms` | INTEGER | NULLABLE |
| `floor` | INTEGER | NULLABLE |
| `is_exterior` | BOOLEAN | NULLABLE |
| `description` | STRING | NULLABLE |
| `image_url` | STRING | NULLABLE |
| `lat` | FLOAT | NULLABLE |
| `lon` | FLOAT | NULLABLE |
| `email_date` | TIMESTAMP | NULLABLE |
| `campaign_type` | STRING | NULLABLE |
| `email_id` | STRING | NULLABLE |
| `_loaded_at` | TIMESTAMP | NULLABLE |

**Column profiles:**

**`property_id`** (STRING):
  - Distinct: 821, Nulls: 0 (0.0%), Empty strings: 0, Length: [8..9]
  - Top values:
    - `110923392`: 2
    - `110256789`: 2
    - `109804987`: 2
    - `110653466`: 2
    - `109805013`: 2
    - `108659248`: 2
    - `110939167`: 2
    - `110933337`: 2
    - `110931942`: 2
    - `106478985`: 2

**`operation_type`** (STRING):
  - Distinct: 2, Nulls: 0 (0.0%), Empty strings: 0, Length: [4..4]
  - Top values:
    - `sale`: 608
    - `rent`: 276

**`property_type`** (STRING):
  - Distinct: 4, Nulls: 142 (16.1%), Empty strings: 0, Length: [4..7]
  - Top values:
    - `Piso`: 644
    - `Dúplex`: 34
    - `Estudio`: 32
    - `Ático`: 32

**`address`** (STRING):
  - Distinct: 612, Nulls: 142 (16.1%), Empty strings: 0, Length: [4..84]
  - Top values:
    - `Zaidín, Granada`: 25
    - `Calle Bernarda Alba, Zaidín, Granada`: 6
    - `Calle Emperatriz Eugenia, 4, Gran Capitán, Granada`: 6
    - `Calle Bernarda Alba, 3, Zaidín, Granada`: 6
    - `Figares, Granada`: 5
    - `Calle Pedro Antonio de Alarcón, 30, Recogidas, Granada`: 5
    - `Calle Numancia, Zaidín, Granada`: 5
    - `San Ildefonso, Granada`: 5
    - `Calle Emperatriz Eugenia, Gran Capitán, Granada`: 4
    - `Ronda - Arabial, Granada`: 4

**`city`** (STRING):
  - Distinct: 67, Nulls: 142 (16.1%), Empty strings: 0, Length: [4..27]
  - Top values:
    - `Madrid`: 385
    - `Granada`: 172
    - `Alcalá de Henares`: 14
    - `Getafe`: 10
    - `Alcorcón`: 9
    - `Móstoles`: 8
    - `San Sebastián de los Reyes`: 7
    - `Churriana de la Vega`: 7
    - `Las Rozas de Madrid`: 6
    - `Leganés`: 6

**`price`** (FLOAT):
  - Min: 400, Max: 6,000,000, Mean: 330,052.76, Median: 209,000, Stddev: 518,157.11
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 16

**`area_m2`** (FLOAT):
  - Min: 0, Max: 899, Mean: 113.78, Median: 86, Stddev: 88.70
  - Nulls: 0 (0.0%), Zeros: 2, Negatives: 0
  - Outliers (>3σ): 18

**`bedrooms`** (INTEGER):
  - Min: 1, Max: 20, Mean: 2.80, Median: 3, Stddev: 1.39
  - Nulls: 43 (4.9%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 8

**`floor`** (INTEGER):
  - Min: 1, Max: 17, Mean: 3.19, Median: 3, Stddev: 2.02
  - Nulls: 292 (33.0%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 5

**`is_exterior`** (BOOLEAN):
  - True: 6, False: 878, Null: 0

**`description`** (STRING):
  - Distinct: 665, Nulls: 0 (0.0%), Empty strings: 0, Length: [103..500]
  - Top values:
    - `¿Este anuncio se ajusta a lo que estás buscando? Desde  Tus búsquedas  puedes re`: 68
    - `Presentamos un excelente piso ubicado en la codiciada zona del Zaidín, Granada. `: 22
    - `DISPONIBLE EN: Mayo. 
Reserve en línea haciendo clic bajo el mapa en "Enlace adi`: 14
    - `PISO OCUPADO POR PERSONA SIN JUSTO TÍTULOESTE INMUEBLE DEBIDO A SU ESTADO OCUPAC`: 14
    - `Piso en venta en pleno centro de Granada Calle Emperatriz Eugenia, junto a Plaza`: 10
    - `Vivienda ideal para quienes buscan luz, tranquilidad y una excelente ubicación. `: 9
    - `Ponemos a la venta este céntrico piso ubicado en una tercera planta de 213 m2 co`: 6
    - `Se vende bonita casa adosada en una de las mejores zonas de Churriana de la Vega`: 6
    - `¿Alguna vez has soñado con vivir dentro de un pedazo de historia?

Esta no es un`: 5
    - `Ponemos a la venta esta fantástica casa reformada ubicada en el corazón de Atarf`: 5

**`image_url`** (STRING):
  - Distinct: 818, Nulls: 0 (0.0%), Empty strings: 0, Length: [62..95]
  - Top values:
    - `http://st1.idealista.com/static/common/mailing/img/no-pics.gif`: 4
    - `https://img4.idealista.com/blur/500_375_mq/0/id.pro.es.image.master/8d/b1/44/142`: 2
    - `https://img4.idealista.com/blur/500_375_mq/0/id.pro.es.image.master/e3/9b/08/140`: 2
    - `https://img4.idealista.com/blur/500_375_mq/0/id.pro.es.image.master/e6/b7/7b/141`: 2
    - `https://img4.idealista.com/blur/500_375_mq/0/id.pro.es.image.master/f5/8d/3b/138`: 2
    - `https://img4.idealista.com/blur/500_375_mq/0/id.pro.es.image.master/74/e6/b2/138`: 2
    - `https://img4.idealista.com/blur/500_375_mq/0/id.pro.es.image.master/d4/ae/9a/138`: 2
    - `https://img4.idealista.com/blur/500_375_mq/0/id.pro.es.image.master/92/c7/54/142`: 2
    - `https://img4.idealista.com/blur/500_375_mq/0/id.pro.es.image.master/a1/72/8f/139`: 2
    - `https://img4.idealista.com/blur/500_375_mq/90/id.pro.es.image.master/ec/70/fd/14`: 2

**`lat`** (FLOAT):
  - Min: 37.09, Max: 41.12, Mean: 39.53, Median: 40.40, Stddev: 1.45
  - Nulls: 142 (16.1%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 0

**`lon`** (FLOAT):
  - Min: -4.96, Max: -3.14, Mean: -3.67, Median: -3.68, Stddev: 0.13
  - Nulls: 142 (16.1%), Zeros: 0, Negatives: 742
  - Outliers (>3σ): 10

**`email_date`** (TIMESTAMP):
  - Range: 2026-03-16 17:06:46+00:00 → 2026-03-17 17:00:43+00:00
  - Nulls: 0 (0.0%), Future dates: 0

**`campaign_type`** (STRING):
  - Distinct: 9, Nulls: 0 (0.0%), Empty strings: 0, Length: [3..35]
  - Top values:
    - `express_newAd_sale_professional`: 304
    - `express_priceDrop_sale_professional`: 220
    - `express_newAd_rent_professional`: 183
    - `express_newAd_rent_particular`: 67
    - `express_newAd_sale_particular`: 65
    - `express_priceDrop_rent_professional`: 23
    - `express_priceDrop_sale_particular`: 11
    - `fvp`: 8
    - `express_priceDrop_rent_particular`: 3

**`email_id`** (STRING):
  - Distinct: 884, Nulls: 0 (0.0%), Empty strings: 0, Length: [16..16]
  - Top values:
    - `19cf874900b161e3`: 1
    - `19cf86c80abd600d`: 1
    - `19cf87931b1127a9`: 1
    - `19cf89789598b36b`: 1
    - `19cf88065a0c2093`: 1
    - `19cf8970cdc56c3e`: 1
    - `19cf879c33ceeffd`: 1
    - `19cf877aa7c5fa95`: 1
    - `19cf8976c2bfa7b1`: 1
    - `19cf88596270c19f`: 1

**`_loaded_at`** (TIMESTAMP):
  - Range: 2026-03-16 20:11:48.111265+00:00 → 2026-03-17 17:09:05.445400+00:00
  - Nulls: 0 (0.0%), Future dates: 0

**Duplicates:** 0 full-row duplicates

**Null patterns:** `property_type`: 142 (16.1%) | `address`: 142 (16.1%) | `city`: 142 (16.1%) | `bedrooms`: 43 (4.9%) | `floor`: 292 (33.0%) | `lat`: 142 (16.1%) | `lon`: 142 (16.1%)


#### Idealista Listings — specific checks

**Operation types (`operation_type`):**

  - `sale`: 608
  - `rent`: 276

**Price column** (`price`, type: FLOAT):

  - Min: 400, Max: 6,000,000, Mean: 330,052.76, Median: 209,000
  - Nulls: 0, Zeros: 0

  Price by operation type:

  - `sale`: min=58,000, max=6,000,000, median=300,000
  - `rent`: min=400, max=18,000, median=1,250

**Area (`area_m2`):** min=0, max=899, avg=113.78, nulls=0, zeros=2, >1000m²=0

**Listings by city:**

  - `Madrid`: 385
  - `Granada`: 172
  - `None`: 142
  - `Alcalá de Henares`: 14
  - `Getafe`: 10
  - `Alcorcón`: 9
  - `Móstoles`: 8
  - `San Sebastián de los Reyes`: 7
  - `Churriana de la Vega`: 7
  - `Las Rozas de Madrid`: 6
  - `Parla`: 6
  - `Pozuelo de Alarcón`: 6
  - `Leganés`: 6
  - `Majadahonda`: 5
  - `Torrejón de Ardoz`: 5
  - `Guadarrama`: 5
  - `Ciempozuelos`: 5
  - `Arganda`: 4
  - `Alhendin`: 4
  - `Alcobendas`: 4
  - `San Fernando de Henares`: 4
  - `Fuenlabrada`: 4
  - `La Moraleja`: 4
  - `Villanueva del Pardillo`: 3
  - `Villaviciosa de Odón`: 3
  - `Collado Villalba`: 3
  - `Torres de la Alameda`: 2
  - `Armilla`: 2
  - `Albolote`: 2
  - `San Martín de la Vega`: 2
  - `La Zubia`: 2
  - `Colmenar Viejo`: 2
  - `Valdemorillo`: 2
  - `Rivas-Vaciamadrid`: 2
  - `Chinchón`: 2
  - `Sierra Nevada`: 2
  - `Las Gabias`: 2
  - `Ogijares`: 1
  - `Monachil`: 1
  - `Atarfe`: 1
  - `Soto del Real`: 1
  - `San Lorenzo de El Escorial`: 1
  - `Buitrago del Lozoya`: 1
  - `Coslada`: 1
  - `Valdemoro`: 1
  - `Maracena`: 1
  - `San Martín de Valdeiglesias`: 1
  - `Pinos-Puente`: 1
  - `Sevilla la Nueva`: 1
  - `Meco`: 1
  - `Mejorada del Campo`: 1
  - `Cijuela`: 1
  - `Otura`: 1
  - `Cullar-Vega`: 1
  - `Cogollos Vega`: 1
  - `Pinos-Genil`: 1
  - `Loeches`: 1
  - `Velilla de San Antonio`: 1
  - `Campo Real`: 1
  - `Cadalso de los Vidrios`: 1
  - `Villarejo de Salvanés`: 1
  - `Moralzarzal`: 1
  - `Pinto`: 1
  - `La Cabrera`: 1
  - `Galapagar`: 1
  - `Camarma de Esteruelas`: 1
  - `Boadilla del Monte`: 1
  - `Villanueva de la Cañada`: 1

**Primary key (`property_id`):** 821 unique out of 884 rows

**Duplicate address+price combos:** 54


### `ine_ipv` (608 rows)

**Schema:**

| Column | Type | Mode |
|--------|------|------|
| `autonomous_community` | STRING | NULLABLE |
| `index_type` | STRING | NULLABLE |
| `period` | STRING | NULLABLE |
| `value` | FLOAT | NULLABLE |
| `_loaded_at` | TIMESTAMP | NULLABLE |

**Column profiles:**

**`autonomous_community`** (STRING):
  - Distinct: 2, Nulls: 0 (0.0%), Empty strings: 0, Length: [12..23]
  - Top values:
    - `01 Andalucía`: 304
    - `13 Madrid, Comunidad de`: 304

**`index_type`** (STRING):
  - Distinct: 4, Nulls: 0 (0.0%), Empty strings: 0, Length: [6..29]
  - Top values:
    - `Variación trimestral`: 152
    - `Índice`: 152
    - `Variación en lo que va de año`: 152
    - `Variación anual`: 152

**`period`** (STRING):
  - Distinct: 76, Nulls: 0 (0.0%), Empty strings: 0, Length: [6..6]
  - Top values:
    - `2024T3`: 8
    - `2023T4`: 8
    - `2023T3`: 8
    - `2024T1`: 8
    - `2025T3`: 8
    - `2025T4`: 8
    - `2025T1`: 8
    - `2024T2`: 8
    - `2024T4`: 8
    - `2025T2`: 8

**`value`** (FLOAT):
  - Min: -17.90, Max: 207.67, Mean: 33.52, Median: 3, Stddev: 57.23
  - Nulls: 0 (0.0%), Zeros: 7, Negatives: 140
  - Outliers (>3σ): 1

**`_loaded_at`** (TIMESTAMP):
  - Range: 2026-03-15 16:47:04.742958+00:00 → 2026-03-15 16:47:04.742958+00:00
  - Nulls: 0 (0.0%), Future dates: 0

**Duplicates:** 0 full-row duplicates

**Null patterns:** No nulls found


#### INE IPV — specific checks

**Columns:** `autonomous_community`, `index_type`, `period`, `value`, `_loaded_at`

**Index types (`index_type`):**

  - `Índice`: 152
  - `Variación trimestral`: 152
  - `Variación anual`: 152
  - `Variación en lo que va de año`: 152

**Period range (`period`):** 2007T1 → 2025T4 (76 distinct)


### `ine_renta` (3,120 rows)

**Schema:**

| Column | Type | Mode |
|--------|------|------|
| `municipality_code` | STRING | NULLABLE |
| `municipality_name` | STRING | NULLABLE |
| `year` | INTEGER | NULLABLE |
| `net_avg_income` | FLOAT | NULLABLE |
| `city` | STRING | NULLABLE |
| `_loaded_at` | TIMESTAMP | NULLABLE |

**Column profiles:**

**`municipality_code`** (STRING):
  - Distinct: 353, Nulls: 0 (0.0%), Empty strings: 0, Length: [5..5]
  - Top values:
    - `18006`: 9
    - `18011`: 9
    - `18012`: 9
    - `18010`: 9
    - `18002`: 9
    - `18001`: 9
    - `18004`: 9
    - `18007`: 9
    - `18005`: 9
    - `18003`: 9

**`municipality_name`** (STRING):
  - Distinct: 353, Nulls: 0 (0.0%), Empty strings: 0, Length: [3..44]
  - Top values:
    - `Albuñol`: 9
    - `Alfacar`: 9
    - `Algarinejo`: 9
    - `Aldeire`: 9
    - `Alamedilla`: 9
    - `Agrón`: 9
    - `Albondón`: 9
    - `Albuñuelas`: 9
    - `Albuñán`: 9
    - `Albolote`: 9

**`year`** (INTEGER):
  - Min: 2,015, Max: 2,023, Mean: 2,019.04, Median: 2,019, Stddev: 2.58
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 0

**`net_avg_income`** (FLOAT):
  - Min: 5,772, Max: 30,524, Mean: 11,230.45, Median: 10,647, Stddev: 3,103.34
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 38

**`city`** (STRING):
  - Distinct: 2, Nulls: 0 (0.0%), Empty strings: 0, Length: [6..7]
  - Top values:
    - `Madrid`: 1,561
    - `Granada`: 1,559

**`_loaded_at`** (TIMESTAMP):
  - Range: 2026-03-15 16:46:00.551373+00:00 → 2026-03-15 16:46:00.551373+00:00
  - Nulls: 0 (0.0%), Future dates: 0

**Duplicates:** 0 full-row duplicates

**Null patterns:** No nulls found


#### INE Renta — specific checks

**Columns:** `municipality_code`, `municipality_name`, `year`, `net_avg_income`, `city`, `_loaded_at`

**Year range (`year`):** 2015 → 2023 (9 distinct)

**Municipality values (`municipality_code`):**

  - `18006`: 9
  - `18011`: 9
  - `18012`: 9
  - `18013`: 9
  - `18015`: 9
  - `18016`: 9
  - `18017`: 9
  - `18020`: 9
  - `18021`: 9
  - `18022`: 9
  - `18023`: 9
  - `18010`: 9
  - `18004`: 9
  - `18002`: 9
  - `18001`: 9
  - `18018`: 9
  - `18014`: 9
  - `18007`: 9
  - `18005`: 9
  - `18003`: 9

**Income values (`net_avg_income`):** min=5,772, max=30,524, avg=11,230.45, nulls=0, zeros=0


### `ministerio_transacciones` (176 rows)

**Schema:**

| Column | Type | Mode |
|--------|------|------|
| `municipality` | STRING | NULLABLE |
| `year` | INTEGER | NULLABLE |
| `quarter` | INTEGER | NULLABLE |
| `transactions` | INTEGER | NULLABLE |
| `_loaded_at` | TIMESTAMP | NULLABLE |

**Column profiles:**

**`municipality`** (STRING):
  - Distinct: 2, Nulls: 0 (0.0%), Empty strings: 0, Length: [6..7]
  - Top values:
    - `Granada`: 88
    - `Madrid`: 88

**`year`** (INTEGER):
  - Min: 2,004, Max: 2,025, Mean: 2,014.50, Median: 2,014, Stddev: 6.36
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 0

**`quarter`** (INTEGER):
  - Min: 1, Max: 4, Mean: 2.50, Median: 2, Stddev: 1.12
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 0

**`transactions`** (INTEGER):
  - Min: 267, Max: 16,648, Mean: 5,051.34, Median: 1,186, Stddev: 4,828.03
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 0

**`_loaded_at`** (TIMESTAMP):
  - Range: 2026-03-16 14:57:55.303519+00:00 → 2026-03-16 14:57:55.303519+00:00
  - Nulls: 0 (0.0%), Future dates: 0

**Duplicates:** 0 full-row duplicates

**Null patterns:** No nulls found


#### Ministerio Transacciones — specific checks

**Columns:** `municipality`, `year`, `quarter`, `transactions`, `_loaded_at`

**Quarter format (`quarter`):** `1`, `2`, `3`, `4` ...

**Transaction values (`transactions`):** min=267, max=16,648, nulls=0

**Cities (`municipality`):**

  - `Granada`: 88
  - `Madrid`: 88

**Sample row:**

```
  municipality: Granada
  year: 2004
  quarter: 1
  transactions: 504
  _loaded_at: 2026-03-16 14:57:55.303519+00:00
```


### `ministerio_valor_tasado` (168 rows)

**Schema:**

| Column | Type | Mode |
|--------|------|------|
| `province` | STRING | NULLABLE |
| `municipality` | STRING | NULLABLE |
| `year` | INTEGER | NULLABLE |
| `quarter` | INTEGER | NULLABLE |
| `appraised_value_eur_m2` | FLOAT | NULLABLE |
| `num_appraisals` | FLOAT | NULLABLE |
| `_loaded_at` | TIMESTAMP | NULLABLE |

**Column profiles:**

**`province`** (STRING):
  - Distinct: 2, Nulls: 0 (0.0%), Empty strings: 0, Length: [6..7]
  - Top values:
    - `Granada`: 84
    - `Madrid`: 84

**`municipality`** (STRING):
  - Distinct: 2, Nulls: 0 (0.0%), Empty strings: 0, Length: [6..7]
  - Top values:
    - `Granada`: 84
    - `Madrid`: 84

**`year`** (INTEGER):
  - Min: 2,005, Max: 2,025, Mean: 2,015, Median: 2,015, Stddev: 6.07
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 0

**`quarter`** (INTEGER):
  - Min: 1, Max: 4, Mean: 2.50, Median: 2, Stddev: 1.12
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 0

**`appraised_value_eur_m2`** (FLOAT):
  - Min: 1,471, Max: 5,920.60, Mean: 2,834.17, Median: 2,646.10, Stddev: 940.64
  - Nulls: 1 (0.6%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 1

**`num_appraisals`** (FLOAT):
  - Min: 405, Max: 16,638, Mean: 3,136.13, Median: 2,194.20, Stddev: 2,958.72
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 5

**`_loaded_at`** (TIMESTAMP):
  - Range: 2026-03-16 14:58:18.168925+00:00 → 2026-03-16 14:58:18.168925+00:00
  - Nulls: 0 (0.0%), Future dates: 0

**Duplicates:** 0 full-row duplicates

**Null patterns:** `appraised_value_eur_m2`: 1 (0.6%)


#### Ministerio Valor Tasado — specific checks

**Columns:** `province`, `municipality`, `year`, `quarter`, `appraised_value_eur_m2`, `num_appraisals`, `_loaded_at`

**Values (`appraised_value_eur_m2`):** min=1,471, max=5,920.60, avg=2,834.17, nulls=1

**Values by city:**

  - `Granada`: min=1,471 €/m², max=2,669.50 €/m², avg=2,042.62 €/m²
  - `Madrid`: min=2,646.10 €/m², max=5,920.60 €/m², avg=3,616.30 €/m²

**Sample row:**

```
  province: Granada
  municipality: Granada
  year: 2005
  quarter: 1
  appraised_value_eur_m2: 1688.2
  num_appraisals: 1115.0
  _loaded_at: 2026-03-16 14:58:18.168925+00:00
```


### `neighborhoods` (197 rows)

**Schema:**

| Column | Type | Mode |
|--------|------|------|
| `city` | STRING | NULLABLE |
| `level` | STRING | NULLABLE |
| `name` | STRING | NULLABLE |
| `code` | STRING | NULLABLE |
| `district_name` | STRING | NULLABLE |
| `geometry_wkt` | STRING | NULLABLE |
| `_loaded_at` | TIMESTAMP | NULLABLE |

**Column profiles:**

**`city`** (STRING):
  - Distinct: 2, Nulls: 0 (0.0%), Empty strings: 0, Length: [6..7]
  - Top values:
    - `Madrid`: 152
    - `Granada`: 45

**`level`** (STRING):
  - Distinct: 2, Nulls: 0 (0.0%), Empty strings: 0, Length: [8..12]
  - Top values:
    - `neighborhood`: 168
    - `district`: 29

**`name`** (STRING):
  - Distinct: 192, Nulls: 0 (0.0%), Empty strings: 0, Length: [3..48]
  - Top values:
    - `Centro`: 2
    - `Zaidín`: 2
    - `Joaquina Eguaras`: 2
    - `La Paz`: 2
    - `San Matías-Realejo`: 2
    - `Campus de la Salud`: 1
    - `Cartuja`: 1
    - `Lancha del Genil`: 1
    - `Bobadilla`: 1
    - `Pajaritos`: 1

**`code`** (STRING):
  - Distinct: 189, Nulls: 8 (4.1%), Empty strings: 0, Length: [2..6]
  - Top values:
    - `ZAI-01`: 1
    - `NOR-01`: 1
    - `NOR-02`: 1
    - `RON-01`: 1
    - `GEN-02`: 1
    - `GEN-01`: 1
    - `CHA-02`: 1
    - `BEI-01`: 1
    - `GEN-03`: 1
    - `CHA-01`: 1

**`district_name`** (STRING):
  - Distinct: 28, Nulls: 29 (14.7%), Empty strings: 0, Length: [5..21]
  - Top values:
    - `Ciudad Lineal`: 9
    - `Centro`: 9
    - `Norte`: 8
    - `Fuencarral - El Pardo`: 8
    - `San Blas - Canillejas`: 8
    - `Latina`: 7
    - `Arganzuela`: 7
    - `Usera`: 7
    - `Moncloa - Aravaca`: 7
    - `Carabanchel`: 7

**`geometry_wkt`** (STRING):
  - Distinct: 197, Nulls: 0 (0.0%), Empty strings: 0, Length: [253..111725]
  - Top values:
    - `MULTIPOLYGON (((-3.595418829793736 37.14995935990504, -3.5954917463234053 37.149`: 1
    - `MULTIPOLYGON (((-3.596912551148048 37.207161820641474, -3.5975818710028933 37.20`: 1
    - `MULTIPOLYGON (((-3.6038962608515717 37.20807662873544, -3.6040040496216945 37.20`: 1
    - `MULTIPOLYGON (((-3.6136406160652363 37.18295852086527, -3.613044291257353 37.181`: 1
    - `MULTIPOLYGON (((-3.5845174146173746 37.15483679582761, -3.58451990824262 37.1548`: 1
    - `MULTIPOLYGON (((-3.5740289159595013 37.155667407679665, -3.574309754735709 37.15`: 1
    - `MULTIPOLYGON (((-3.6269219613138772 37.205298508527505, -3.6274293458485873 37.2`: 1
    - `MULTIPOLYGON (((-3.6114641365069082 37.18905302808224, -3.6115840717675582 37.18`: 1
    - `MULTIPOLYGON (((-3.5655392619158 37.16292155192136, -3.5655791872780176 37.16315`: 1
    - `MULTIPOLYGON (((-3.64232978613574 37.189197448641224, -3.6420586791550162 37.188`: 1

**`_loaded_at`** (TIMESTAMP):
  - Range: 2026-03-17 17:41:55.804060+00:00 → 2026-03-17 17:41:55.804060+00:00
  - Nulls: 0 (0.0%), Future dates: 0

**Duplicates:** 0 full-row duplicates

**Null patterns:** `code`: 8 (4.1%) | `district_name`: 29 (14.7%)


#### Neighborhoods — specific checks

**Counts by city/level:**

| City | Level | Count |
|------|-------|-------|
| Granada | district | 8 |
| Granada | neighborhood | 37 |
| Madrid | district | 21 |
| Madrid | neighborhood | 131 |

**district_name:** 168 neighborhoods have parent district, 0 missing

**Geometry types:** MULTIPOLYGON: 60, POLYGON: 136, GEOMETRYCOLLECTION: 1

**ST_GEOGFROMTEXT:** 0 out of 197 failed to parse


### `osm_pois` (8,060 rows)

**Schema:**

| Column | Type | Mode |
|--------|------|------|
| `osm_id` | INTEGER | NULLABLE |
| `city` | STRING | NULLABLE |
| `category` | STRING | NULLABLE |
| `osm_type` | STRING | NULLABLE |
| `name` | STRING | NULLABLE |
| `lat` | FLOAT | NULLABLE |
| `lon` | FLOAT | NULLABLE |
| `_loaded_at` | TIMESTAMP | NULLABLE |

**Column profiles:**

**`osm_id`** (INTEGER):
  - Min: 73,006, Max: 13,649,509,640, Mean: 3,265,955,453.07, Median: 2,864,026,501, Stddev: 3,645,802,814.22
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 0

**`city`** (STRING):
  - Distinct: 2, Nulls: 0 (0.0%), Empty strings: 0, Length: [6..7]
  - Top values:
    - `madrid`: 7,346
    - `granada`: 714

**`category`** (STRING):
  - Distinct: 4, Nulls: 0 (0.0%), Empty strings: 0, Length: [6..9]
  - Top values:
    - `health`: 3,038
    - `education`: 2,672
    - `shopping`: 1,300
    - `transport`: 1,050

**`osm_type`** (STRING):
  - Distinct: 14, Nulls: 0 (0.0%), Empty strings: 0, Length: [4..15]
  - Top values:
    - `pharmacy`: 2,264
    - `school`: 1,715
    - `supermarket`: 1,200
    - `kindergarten`: 751
    - `subway_entrance`: 654
    - `clinic`: 492
    - `station`: 376
    - `doctors`: 170
    - `hospital`: 112
    - `university`: 110

**`name`** (STRING):
  - Distinct: 4,909, Nulls: 1,722 (21.4%), Empty strings: 0, Length: [1..140]
  - Top values:
    - `Dia`: 209
    - `Mercadona`: 131
    - `Ahorramás`: 123
    - `Lidl`: 58
    - `Covirán`: 56
    - `Alcampo`: 48
    - `Dia Market`: 40
    - `Aldi`: 28
    - `Carrefour`: 25
    - `Supercor`: 24

**`lat`** (FLOAT):
  - Min: 37.12, Max: 40.56, Mean: 40.13, Median: 40.42, Stddev: 0.92
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 0
  - Outliers (>3σ): 714

**`lon`** (FLOAT):
  - Min: -3.84, Max: -3.52, Mean: -3.68, Median: -3.68, Stddev: 0.06
  - Nulls: 0 (0.0%), Zeros: 0, Negatives: 8,060
  - Outliers (>3σ): 0

**`_loaded_at`** (TIMESTAMP):
  - Range: 2026-03-15 19:53:14.816936+00:00 → 2026-03-15 19:55:49.221596+00:00
  - Nulls: 0 (0.0%), Future dates: 0

**Duplicates:** 0 full-row duplicates

**Null patterns:** `name`: 1,722 (21.4%)


#### OSM POIs — specific checks

**Columns available:** `osm_id`, `city`, `category`, `osm_type`, `name`, `lat`, `lon`, `_loaded_at`

**Categories:**

  - `health`: 3,038
  - `education`: 2,672
  - `shopping`: 1,300
  - `transport`: 1,050

**Names:** 1,722 null, 0 empty out of 8,060

**Coordinates outside Spain:** 0

**POIs by city:**

  - `madrid`: 7,346
  - `granada`: 714


## Cross-table consistency checks

### City naming conventions

- `neighborhoods.city`: `Granada`, `Madrid`
- `osm_pois.city`: `granada`, `madrid`
- `idealista_listings.city`: `None`, `Albolote`, `Alcalá de Henares`, `Alcobendas`, `Alcorcón`, `Alhendin`, `Arganda`, `Armilla`, `Atarfe`, `Boadilla del Monte`, `Buitrago del Lozoya`, `Cadalso de los Vidrios`, `Camarma de Esteruelas`, `Campo Real`, `Chinchón`, `Churriana de la Vega`, `Ciempozuelos`, `Cijuela`, `Cogollos Vega`, `Collado Villalba`, `Colmenar Viejo`, `Coslada`, `Cullar-Vega`, `Fuenlabrada`, `Galapagar`, `Getafe`, `Granada`, `Guadarrama`, `La Cabrera`, `La Moraleja`, `La Zubia`, `Las Gabias`, `Las Rozas de Madrid`, `Leganés`, `Loeches`, `Madrid`, `Majadahonda`, `Maracena`, `Meco`, `Mejorada del Campo`, `Monachil`, `Moralzarzal`, `Móstoles`, `Ogijares`, `Otura`, `Parla`, `Pinos-Genil`, `Pinos-Puente`, `Pinto`, `Pozuelo de Alarcón`, `Rivas-Vaciamadrid`, `San Fernando de Henares`, `San Lorenzo de El Escorial`, `San Martín de Valdeiglesias`, `San Martín de la Vega`, `San Sebastián de los Reyes`, `Sevilla la Nueva`, `Sierra Nevada`, `Soto del Real`, `Torrejón de Ardoz`, `Torres de la Alameda`, `Valdemorillo`, `Valdemoro`, `Velilla de San Antonio`, `Villanueva de la Cañada`, `Villanueva del Pardillo`, `Villarejo de Salvanés`, `Villaviciosa de Odón`
- `ministerio_transacciones.municipality`: `Granada`, `Madrid`
- `ministerio_valor_tasado.municipality`: `Granada`, `Madrid`
