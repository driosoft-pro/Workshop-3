---
name: data-harmonization
description: Hechos ya perfilados de los CSV 2015-2019, mapeo de columnas, alias de países, decisiones de limpieza y suite de Great Expectations. Usar en EDA, limpieza, src/harmonize.py y validación batch. Evita re-perfilar los datos.
---
# Datos ya perfilados (no volver a perfilar)
Filas 158/157/155/156/156 = **782**. 0 duplicados, 0 `(country,year)` repetidos. Único nulo: `corruption` de **United Arab Emirates 2018**. Todos los indicadores `float64`. Hay ceros exactos reales (Sierra Leone, Lesotho 2017, Swaziland 2019): conservar. Outlier plausible: `generosity`≈0.838 (2017): conservar. `region` solo en 2015-16. El año NO está en los archivos → tomar del nombre.
Correlación con score: gdp .79 · health .74 · family .65 · freedom .55 · corruption .40 · generosity .14.

## Mapeo de columnas → esquema unificado
```python
RENAME = {
 2015: {"Country":"country","Region":"region","Happiness Score":"happiness_score","Economy (GDP per Capita)":"gdp","Family":"family","Health (Life Expectancy)":"health","Freedom":"freedom","Trust (Government Corruption)":"corruption","Generosity":"generosity"},
 2016: "igual que 2015",
 2017: {"Country":"country","Happiness.Score":"happiness_score","Economy..GDP.per.Capita.":"gdp","Family":"family","Health..Life.Expectancy.":"health","Freedom":"freedom","Trust..Government.Corruption.":"corruption","Generosity":"generosity"},
 2018: {"Country or region":"country","Score":"happiness_score","GDP per capita":"gdp","Social support":"family","Healthy life expectancy":"health","Freedom to make life choices":"freedom","Perceptions of corruption":"corruption","Generosity":"generosity"},
 2019: "igual que 2018",
}
KEEP = ["country","year","gdp","family","health","freedom","generosity","corruption","happiness_score"]  # +region solo para dim_country
```
**Excluir (fuga/derivadas):** Rank, Standard Error, Lower/Upper Confidence Interval, Whisker.high/low, Dystopia Residual.

## Alias de países (aplicar antes del merge; guardar en `data/reference/country_aliases.csv`)
`Trinidad & Tobago→Trinidad and Tobago` · `Hong Kong S.A.R., China→Hong Kong` · `Taiwan Province of China→Taiwan` · `Macedonia→North Macedonia` · `Northern Cyprus→North Cyprus` · `Somaliland Region→Somaliland region`. Además `strip()` y colapsar espacios. Verificar con `groupby('country').year.nunique()`; revisar los países que no estén en los 5 años por si queda otra variante.
Región por país: construir de 2015-2016 y propagar; países sin región → `"Unknown"` (documentar). **region NO es feature.**

## Decisiones (documentar Problema→Decisión→Justificación)
- Nulo UAE-2018: imputar con media de UAE 2017 y 2019; `SimpleImputer(median)` en el Pipeline como red de seguridad.
- Ceros/outliers: marcar y conservar (valores reales; objetivo no es optimizar).
- Formato largo (una fila país-año) con `pd.concat`; nunca merge ancho.
- Tipos: country/region `string`, year `int`, resto `float64` (`pd.to_numeric(errors="raise")`).
- Escala 2017 ligeramente distinta a 2018-19: anotar como limitación.

## Great Expectations (sobre el unificado, ejecutada en el DAG)
columnas en orden · no nulos (country, year, score, 6 features) · year ∈ 2015..2019 · rangos gdp 0-2.5, family 0-2, health 0-1.5, freedom/generosity/corruption 0-1, score 0-10 · (country,year) único · tipos. La API de GE cambia entre 0.x y 1.x: **fijar versión** en requirements.

## Salidas
`data/processed/unified_happiness.parquet` (+csv) · `src/harmonize.py: build_unified() -> DataFrame` (lo usan notebook y Airflow; una sola implementación).
