"""
Docstring for clima.src.synthetic_data.calentamieto_global

Deterimar tasa de cambio de temperatura por década.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, avg as spark_avg
import numpy as np
import pandas as pd
import os

HDFS_INPUT = "hdfs://namenode:9000/clima/GlobalLandTemperaturesByCity.csv"
HDFS_OUTPUT_DIR = "hdfs://namenode:9000/clima/calentamiento_global.csv"

def _detect_columns(df):
    date_col = next((c for c in df.columns if 'date' in c.lower() or c.lower()=='dt'), None)
    temp_col = next((c for c in df.columns if 'temp' in c.lower()), None)
    city_col = next((c for c in df.columns if 'city' in c.lower()), None)
    country_col = next((c for c in df.columns if 'country' in c.lower()), None)
    lat_col = next((c for c in df.columns if 'lat' in c.lower()), None)
    lon_col = next((c for c in df.columns if 'lon' in c.lower()), None)
    return date_col, temp_col, city_col, country_col, lat_col, lon_col

def generate_warming_rate(spark: SparkSession):
    """Calcula la tasa de cambio de temperatura por década para cada ciudad."""
    df = spark.read.csv(HDFS_INPUT, header=True, inferSchema=True)
    date_col, temp_col, city_col, country_col, lat_col, lon_col = _detect_columns(df)
    if not all([date_col, temp_col, city_col]):
        raise RuntimeError("No se detectaron columnas date/temperature/city en el dataset original.")
    df = df.withColumn("date", to_date(col(date_col)))
    df_year = df.withColumn("year", year(col("date"))).groupBy(city_col, country_col, "year").agg(spark_avg(temp_col).alias("annual_avg"))
    # collect per city to pandas, fit linear slope
    rows = []
    for city, g in df_year.groupBy(city_col, country_col).agg():
        pass
    # simpler: get list of cities then iterate
    cities = [r[0] for r in df_year.select(city_col).distinct().collect()]
    for city in cities:
        city_df = df_year.filter(col(city_col) == city).select("year", "annual_avg").toPandas().dropna()
        if city_df.shape[0] < 2:
            continue
        years = city_df["year"].astype(int).values
        temps = city_df["annual_avg"].astype(float).values
        # linear fit
        slope, intercept = np.polyfit(years, temps, 1)
        slope_per_decade = slope * 10.0
        # fetch country, lat/lon if present
        country = df.filter(col(city_col) == city).select(country_col).limit(1).collect()[0][0] if country_col else None
        lat = df.filter(col(city_col) == city).select(lat_col).limit(1).collect()[0][0] if lat_col else None
        lon = df.filter(col(city_col) == city).select(lon_col).limit(1).collect()[0][0] if lon_col else None
        rows.append({
            "city": city,
            "country": country,
            "latitude": lat,
            "longitude": lon,
            "slope_per_decade": float(slope_per_decade),
            "slope_per_year": float(slope)
        })
    if not rows:
        empty = spark.createDataFrame([], schema="city string, country string, latitude string, longitude string, slope_per_decade double, slope_per_year double")
        empty.coalesce(1).write.mode("overwrite").option("header","true").csv(HDFS_OUTPUT_DIR)
        return
    pdf = pd.DataFrame(rows)
    # convert to spark df and write
    out = spark.createDataFrame(pdf)
    try:
        out.coalesce(1).write.mode("overwrite").option("header","true").csv(HDFS_OUTPUT_DIR)
    except Exception:
        out.write.mode("overwrite").parquet(HDFS_OUTPUT_DIR.replace(".csv",".parquet"))
    return out

def generate_all(spark: SparkSession):
    return generate_warming_rate(spark)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("synthetic_calentamiento_global").getOrCreate()
    generate_warming_rate(spark)
