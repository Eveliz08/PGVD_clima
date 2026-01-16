"""
Criterios para determinar una ola de calor:
    Temperatura mayor que el percentil 90 de la serie histórica de temperatura en ese lugar en esa fecha
    Duración mínima de 3 días consecutivos.
    Tal vez se pueda tener en cuenta la tasa de calentamiento global de alguna forma

Crear dataset y almacenar en HDFS.
nombre del dataset: olas de calor, cada fila representa la ocurrencia de este fenómeno y recoge la información:
fecha de inicio del fenómeno
fecha de fin del fenómeno
País
Ciudad
Promedio de temperatura
Temperatura max alcanzada en estos días.
Humedad
Sequía térmica (Sí o No)
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_date, dayofyear, year, month, lag, min as spark_min,
    max as spark_max, avg as spark_avg, expr, when, rand, lit, sum as spark_sum
)
import os

HDFS_INPUT = "hdfs://namenode:9000/clima/GlobalLandTemperaturesByCity.csv"
HDFS_OUTPUT_DIR = "hdfs://namenode:9000/clima/olas_de_calor.csv"

def _detect_columns(df):
    # detectar columna fecha y temp, ciudad, country, lat/lon
    date_col = next((c for c in df.columns if 'date' in c.lower() or c.lower()=='dt'), None)
    temp_col = next((c for c in df.columns if 'temp' in c.lower()), None)
    city_col = next((c for c in df.columns if 'city' in c.lower()), None)
    country_col = next((c for c in df.columns if 'country' in c.lower()), None)
    lat_col = next((c for c in df.columns if 'lat' in c.lower()), None)
    lon_col = next((c for c in df.columns if 'lon' in c.lower()), None)
    return date_col, temp_col, city_col, country_col, lat_col, lon_col

def generate_heat_waves(spark: SparkSession):
    """Genera eventos de olas de calor y los escribe en HDFS."""
    df = spark.read.csv(HDFS_INPUT, header=True, inferSchema=True)
    date_col, temp_col, city_col, country_col, lat_col, lon_col = _detect_columns(df)
    if not all([date_col, temp_col, city_col]):
        raise RuntimeError("No se detectaron columnas date/temperature/city en el dataset original.")
    # preparar columnas
    df = df.withColumn("date", to_date(col(date_col)))
    # day of year to capture seasonality; if daily not available, month-day grouping still works
    df = df.withColumn("doy", dayofyear(col("date")))
    # compute p90 per city and day-of-year
    p90 = df.groupBy(city_col, "doy").agg(expr(f"percentile_approx({temp_col}, 0.9) as p90"))
    df2 = df.join(p90, on=[city_col, "doy"], how="left")
    df2 = df2.withColumn("is_hot", col(temp_col) > col("p90"))
    # compute group id for consecutive segments: cumulative count of not-hot rows
    w = Window.partitionBy(city_col).orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    df2 = df2.withColumn("_not_hot_cumsum", spark_sum(when(~col("is_hot"), 1).otherwise(0)).over(w))
    # group identifier: city + _not_hot_cumsum; only keep hot rows then aggregate
    hot_rows = df2.filter(col("is_hot"))
    if hot_rows.rdd.isEmpty():
        # write empty schema file
        empty = spark.createDataFrame([], schema="start_date date, end_date date, country string, city string, avg_temp double, max_temp double, humidity double, drought string")
        empty.coalesce(1).write.mode("overwrite").option("header","true").csv(HDFS_OUTPUT_DIR)
        return
    grouped = hot_rows.groupBy(city_col, "_not_hot_cumsum").agg(
        spark_min("date").alias("start_date"),
        spark_max("date").alias("end_date"),
        spark_avg(temp_col).alias("avg_temp"),
        spark_max(temp_col).alias("max_temp"),
        expr(f"first({country_col})").alias("country")
    )
    # compute duration days
    grouped = grouped.withColumn("duration_days", expr("datediff(end_date, start_date) + 1"))
    # filter duration >=3
    events = grouped.filter(col("duration_days") >= 3)
    # synthesize humidity and drought: humidity inversely correlated with avg_temp (simple heuristic)
    events = events.withColumn("humidity", expr("round(50 - avg_temp*0.3 + rand()*20,1)"))
    events = events.withColumn("drought", when(col("humidity") < 40, lit("Sí")).otherwise(lit("No")))
    # final select and write
    out = events.select("start_date", "end_date", "country", city_col.alias("city"), "avg_temp", "max_temp", "humidity", "drought")
    try:
        out.coalesce(1).write.mode("overwrite").option("header","true").csv(HDFS_OUTPUT_DIR)
    except Exception as e:
        # intentar parquet como fallback
        out.write.mode("overwrite").parquet(HDFS_OUTPUT_DIR.replace(".csv",".parquet"))
    return out

def generate_all(spark: SparkSession):
    return generate_heat_waves(spark)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("synthetic_olas_de_calor").getOrCreate()
    generate_heat_waves(spark)