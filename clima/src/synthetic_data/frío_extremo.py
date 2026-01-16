"""
Docstring for clima.src.synthetic_data.frío_extremo
Criterios para determinar una ola de frío
Temperatura por debajo del percentil 10
Caída abrupta de temperatura, más de 5 grados en 24 horas

Crear dataset y almacenar en HDFS.
nombre del dataset: olas de frío, cada fila representa la ocurrencia de este fenómeno y recoge la información:
fecha de inicio del fenómeno
fecha de fin del fenómeno
País
Ciudad
Promedio de temperatura
Temperatura min alcanzada en estos días.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_date, dayofyear, lag, lead, min as spark_min,
    max as spark_max, avg as spark_avg, expr, when, lit, rand
)
import os

HDFS_INPUT = "hdfs://namenode:9000/clima/GlobalLandTemperaturesByCity.csv"
HDFS_OUTPUT_DIR = "hdfs://namenode:9000/clima/frio_extremo.csv"

def _detect_columns(df):
    date_col = next((c for c in df.columns if 'date' in c.lower() or c.lower()=='dt'), None)
    temp_col = next((c for c in df.columns if 'temp' in c.lower()), None)
    city_col = next((c for c in df.columns if 'city' in c.lower()), None)
    country_col = next((c for c in df.columns if 'country' in c.lower()), None)
    return date_col, temp_col, city_col, country_col

def generate_cold_extremes(spark: SparkSession):
    df = spark.read.csv(HDFS_INPUT, header=True, inferSchema=True)
    date_col, temp_col, city_col, country_col = _detect_columns(df)
    if not all([date_col, temp_col, city_col]):
        raise RuntimeError("No se detectaron columnas date/temperature/city en el dataset original.")
    df = df.withColumn("date", to_date(col(date_col)))
    df = df.withColumn("doy", dayofyear(col("date")))
    # compute p10 per city and day
    p10 = df.groupBy(city_col, "doy").agg(expr(f"percentile_approx({temp_col}, 0.1) as p10"))
    df2 = df.join(p10, on=[city_col, "doy"], how="left")
    df2 = df2.withColumn("is_cold", col(temp_col) < col("p10"))
    # abrupt drop: previous temp - current temp > 5
    w = Window.partitionBy(city_col).orderBy("date")
    df2 = df2.withColumn("prev_temp", lag(temp_col).over(w))
    df2 = df2.withColumn("abrupt_drop", (col("prev_temp") - col(temp_col)) > 5)
    # mark event rows where is_cold or abrupt_drop
    df2 = df2.withColumn("event_flag", col("is_cold") | col("abrupt_drop"))
    # group consecutive event rows
    w2 = Window.partitionBy(city_col).orderBy("date").rowsBetween(Window.unboundedPreceding,0)
    df2 = df2.withColumn("_non_event_cumsum", expr("sum(case when event_flag then 0 else 1 end) over (partition by %s order by date rows between unbounded preceding and current row)" % city_col))
    events_rows = df2.filter(col("event_flag"))
    if events_rows.rdd.isEmpty():
        empty = spark.createDataFrame([], schema="start_date date, end_date date, country string, city string, avg_temp double, min_temp double")
        empty.coalesce(1).write.mode("overwrite").option("header","true").csv(HDFS_OUTPUT_DIR)
        return
    grouped = events_rows.groupBy(city_col, "_non_event_cumsum").agg(
        spark_min("date").alias("start_date"),
        spark_max("date").alias("end_date"),
        spark_avg(temp_col).alias("avg_temp"),
        spark_min(temp_col).alias("min_temp"),
        expr(f"first({country_col})").alias("country")
    )
    out = grouped.select("start_date", "end_date", "country", city_col.alias("city"), "avg_temp", "min_temp")
    try:
        out.coalesce(1).write.mode("overwrite").option("header","true").csv(HDFS_OUTPUT_DIR)
    except Exception:
        out.write.mode("overwrite").parquet(HDFS_OUTPUT_DIR.replace(".csv",".parquet"))
    return out

def generate_all(spark: SparkSession):
    return generate_cold_extremes(spark)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("synthetic_frio_extremo").getOrCreate()
    generate_cold_extremes(spark)