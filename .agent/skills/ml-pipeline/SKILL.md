---
name: ml-pipeline
description: Selección de features sin fuga, Pipeline de scikit-learn, split 70/30 por país, métricas, guardado de model.pkl y generación de los archivos de streaming. Usar en model_training y src/train.py.
---
# Reglas
- Features (orden fijo): `gdp, family, health, freedom, generosity, corruption`. Importar `FEATURES` de `src/contract.py`.
- NO usar: rank, dystopia residual, error estándar/intervalos, `region` (no está en el contrato), `country` (alta cardinalidad), `year`. Justificar en el README.
- Modelo simple: `LinearRegression` (alternativa `RandomForestRegressor`). Elegir el más simple si la diferencia es pequeña.
- Todo el preprocesamiento va **dentro** del `Pipeline` serializado (nada de escalar fuera).

```python
pipe = Pipeline([
  ("prep", ColumnTransformer([("num", Pipeline([("imp", SimpleImputer(strategy="median")),
                                                ("sc", StandardScaler())]), FEATURES)], remainder="drop")),
  ("model", LinearRegression())])
```
## Split
Cada país aparece hasta 5 veces → usar `GroupShuffleSplit(test_size=0.3, random_state=42)` con `groups=country`. Opcional: reportar también el split aleatorio como comparación.

## Referencia (para detectar errores)
LinReg split por país: MAE≈0.39 RMSE≈0.52 R²≈0.80 · LinReg aleatorio: MAE≈0.45 R²≈0.73 · RF aleatorio R²≈0.78. R²≈0.99 ⇒ sospechar fuga; R²<0.5 ⇒ sospechar columnas desalineadas.

## Artefactos
- `models/model.pkl` (`joblib.dump`), `models/model_metadata.json`: `model_version`, `algorithm`, `features`, `mae`, `rmse`, `r2`, `trained_at`, `sklearn_version`.
- `data/streaming/stream_events.csv`: filas del **test set** con columnas `country,year,gdp,...,happiness_score` (evita KPIs optimistas por predecir sobre train; si se transmite todo, declararlo en el README).
- `data/streaming/invalid_events.csv`: eventos malos a propósito (campo faltante, string en número, NaN, negativo, fecha mala, null) para la demo.
## Verificación
`python -c "import joblib,pandas as pd;m=joblib.load('models/model.pkl');print(m.predict(pd.DataFrame([[1.2,.8,.9,.6,.3,.1]],columns=['gdp','family','health','freedom','generosity','corruption'])))"`
