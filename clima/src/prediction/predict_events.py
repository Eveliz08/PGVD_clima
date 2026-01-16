"""
Entrenamiento y predicción de eventos meteorológicos sintéticos.

Funciones principales:
- train_models(spark, hdfs_original, hdfs_event_paths, ...): entrena un modelo por cada evento y devuelve diccionario con modelos y encoders.
- predict_event(models_dict, sample, threshold=0.5): evalúa un input {'date','city','country'} y devuelve el evento con mayor probabilidad por encima del umbral.

Notas:
- Se usa sklearn (LogisticRegression) para clasificación y OneHotEncoder para variables categóricas.
- Los datasets de eventos deben existir en HDFS en las rutas proporcionadas; los archivos se asumen CSV con columnas start_date,end_date,city,country,...
"""
# filepath: /home/eveliz/Documentos/Libros4to/proyectos/PGVD/Nueva carpeta/PGVD_clima/clima/src/prediction/predict_events.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, min as spark_min, max as spark_max
import pandas as pd
import numpy as np
from datetime import timedelta
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
import os
import logging

logger = logging.getLogger(__name__)

# Rutas HDFS por convención (puedes ajustar si cambias nombres)
HDFS_ORIGINAL = "hdfs://namenode:9000/clima/GlobalLandTemperaturesByCity.csv"
EVENT_PATHS = {
    "olas_de_calor": "hdfs://namenode:9000/clima/olas_de_calor.csv",
    "frio_extremo": "hdfs://namenode:9000/clima/frio_extremo.csv",
    "calentamiento_global": "hdfs://namenode:9000/clima/calentamiento_global.csv"
}

def _spark_read_to_pd(spark, path, infer_dates=None):
    """Lee CSV desde HDFS con Spark y lo devuelve como pandas DataFrame."""
    df = spark.read.csv(path, header=True, inferSchema=True)
    pdf = df.toPandas()
    if infer_dates:
        for c in infer_dates:
            if c in pdf.columns:
                pdf[c] = pd.to_datetime(pdf[c], errors='coerce')
    return pdf

def _expand_event_rows_to_dates(pdf, start_col="start_date", end_col="end_date"):
    """Expande filas con rangos de fechas a filas por fecha (un día por fila)."""
    rows = []
    if pdf.empty:
        return pd.DataFrame(columns=["date","city","country"])
    for _, r in pdf.iterrows():
        try:
            start = pd.to_datetime(r[start_col])
            end = pd.to_datetime(r[end_col])
            if pd.isna(start) or pd.isna(end):
                continue
            for d in pd.date_range(start, end):
                rows.append({"date": d, "city": r.get("city"), "country": r.get("country")})
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=["date","city","country"])
    return pd.DataFrame(rows)

def _build_feature_df(dates_df):
    """Construye características numéricas/categóricas a partir de columnas date/city/country."""
    if dates_df.empty:
        return dates_df
    df = dates_df.copy()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["dayofyear"] = df["date"].dt.dayofyear
    # Keep city and country as categorical strings
    df["city"] = df["city"].astype(str)
    df["country"] = df["country"].astype(str)
    return df[["date","year","month","dayofyear","city","country"]]

def _sample_negatives(positive_df, global_min_date, global_max_date, neg_ratio=2):
    """Genera ejemplos negativos para cada ciudad muestreando fechas sin evento."""
    neg_rows = []
    if positive_df.empty:
        return pd.DataFrame(columns=["date","city","country"])
    cities = positive_df["city"].unique()
    for city in cities:
        # dates where positive is true for this city
        pos_dates = set(pd.to_datetime(positive_df[positive_df["city"]==city]["date"]).dt.date)
        n_pos = len(pos_dates)
        n_neg = max(1, int(n_pos * neg_ratio))
        # sample random dates in range, avoid positives
        attempts = 0
        sampled = set()
        while len(sampled) < n_neg and attempts < n_neg * 10:
            rand_days = np.random.randint(0, (global_max_date - global_min_date).days + 1, size=(n_neg*2,))
            for rd in rand_days:
                d = (global_min_date + timedelta(days=int(rd)))
                if d not in pos_dates and d not in sampled:
                    sampled.add(d)
                    if len(sampled) >= n_neg:
                        break
            attempts += 1
        for d in sampled:
            neg_rows.append({"date": pd.Timestamp(d), "city": city, "country": positive_df[positive_df["city"]==city]["country"].iloc[0]})
    if not neg_rows:
        return pd.DataFrame(columns=["date","city","country"])
    return pd.DataFrame(neg_rows)

def _prepare_training_data(spark, event_hdfs_path, original_hdfs):
    """Prepara X,y para entrenar un modelo del evento."""
    # leer evento y expandir a fechas positivas
    event_pdf = _spark_read_to_pd(spark, event_hdfs_path, infer_dates=["start_date","end_date"])
    pos_pdf = _expand_event_rows_to_dates(event_pdf, "start_date", "end_date")
    if pos_pdf.empty:
        return None, None
    # get overall date range from original dataset to sample negatives
    orig_df = spark.read.csv(original_hdfs, header=True, inferSchema=True).select(to_date(col("dt")).alias("date")) if "dt" in spark.read.csv(original_hdfs, header=True, inferSchema=True).columns else None
    # fallback: derive min/max from event data if original not found
    try:
        global_min = pd.to_datetime(event_pdf['start_date']).min().date()
        global_max = pd.to_datetime(event_pdf['end_date']).max().date()
    except Exception:
        global_min = pd.Timestamp("2000-01-01").date()
        global_max = pd.Timestamp("2020-12-31").date()
    # sample negatives
    neg_pdf = _sample_negatives(pos_pdf, global_min, global_max, neg_ratio=2)
    # labels
    pos_pdf["label"] = 1
    neg_pdf["label"] = 0
    all_df = pd.concat([pos_pdf, neg_pdf], ignore_index=True)
    all_df = _build_feature_df(all_df)
    X = all_df[["year","month","dayofyear","city","country"]]
    y = all_df["label"].values
    return X, y

def train_models(spark: SparkSession, event_paths=EVENT_PATHS, original_hdfs=HDFS_ORIGINAL):
    """
    Entrena modelos por cada evento. Retorna dict:
    {
      'olas_de_calor': {'model': pipeline, 'features': ['year','month','dayofyear','city','country']},
      ...
    }
    """
    models = {}
    for evt_name, path in event_paths.items():
        try:
            X, y = _prepare_training_data(spark, path, original_hdfs)
            if X is None or X.empty:
                logger.warning(f"No hay ejemplos positivos para evento {evt_name}, se omite entrenamiento.")
                continue
            # Split features
            numeric_feats = ["year","month","dayofyear"]
            cat_feats = ["city","country"]
            # Preprocessing: scale numeric, one-hot encode categories (handle unknown)
            numeric_transformer = StandardScaler()
            cat_transformer = OneHotEncoder(handle_unknown='ignore', sparse=False)
            preproc = ColumnTransformer(
                transformers=[
                    ("num", numeric_transformer, numeric_feats),
                    ("cat", cat_transformer, cat_feats)
                ], remainder='drop', sparse_threshold=0
            )
            clf = LogisticRegression(max_iter=200, solver='liblinear')
            pipeline = Pipeline(steps=[("pre", preproc), ("clf", clf)])
            pipeline.fit(X, y)
            models[evt_name] = {
                "pipeline": pipeline,
                "numeric_feats": numeric_feats,
                "cat_feats": cat_feats
            }
            logger.info(f"Modelo entrenado para evento {evt_name} (positivos={int(y.sum())}, total={len(y)})")
        except Exception as e:
            logger.exception(f"Error entrenando modelo para {evt_name}: {e}")
    return models

def _build_input_features(sample, numeric_feats, cat_feats):
    """Construye DataFrame de características a partir de sample {'date','city','country'}."""
    d = {}
    date = pd.to_datetime(sample.get("date"))
    d["year"] = [date.year]
    d["month"] = [date.month]
    d["dayofyear"] = [date.dayofyear]
    d["city"] = [str(sample.get("city"))]
    d["country"] = [str(sample.get("country"))]
    return pd.DataFrame(d)

def predict_event(models_dict, sample, threshold=0.5):
    """
    Evalúa cada modelo y devuelve (event_name, probability, details) para el evento con mayor probabilidad si supera threshold.
    sample: dict con keys 'date' (str/ts), 'city', 'country'
    """
    best = {"event": None, "prob": 0.0, "model": None}
    for evt, info in models_dict.items():
        pipeline = info["pipeline"]
        numeric_feats = info["numeric_feats"]
        cat_feats = info["cat_feats"]
        Xs = _build_input_features(sample, numeric_feats, cat_feats)
        try:
            proba = pipeline.predict_proba(Xs)[0][1]  # prob of positive class
        except Exception as e:
            logger.exception(f"Error predict proba for {evt}: {e}")
            continue
        if proba > best["prob"]:
            best = {"event": evt, "prob": float(proba), "model": pipeline}
    if best["prob"] >= threshold:
        return {"event": best["event"], "probability": best["prob"]}
    return {"event": None, "probability": best["prob"]}

# Simple CLI helpers for manual testing
def train_and_save_dummy(spark):
    models = train_models(spark)
    # no persistence implemented; return models dict
    return models

if __name__ == "__main__":
    spark = SparkSession.builder.appName("predict_events").getOrCreate()
    models = train_models(spark)
    # ejemplo de predicción
    sample = {"date": "2013-07-15", "city": "Santiago", "country": "Chile"}
    print(predict_event(models, sample, threshold=0.5))
