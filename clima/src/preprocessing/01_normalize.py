"""
Docstring for preprocessing.01_normalize

1. Estandarizar el formato de los datos:
    Que todas las fechas estén de la forma día/mes/año.
    Que los valores numéricos continuos estén determinados con punto.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, regexp_replace, when, isnan, isnull, 
    coalesce, lit
)
from pyspark.sql.types import DoubleType
import logging

logger = logging.getLogger(__name__)

class DataNormalizer:
    """Normaliza formatos de fechas y números en datos de HDFS."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def normalize_dates(self, df, date_columns):
        """Convierte todas las fechas al formato dd/mm/yyyy."""
        for col_name in date_columns:
            # Intenta múltiples formatos comunes
            df = df.withColumn(
                col_name,
                coalesce(
                    to_date(col(col_name), "dd/MM/yyyy"),
                    to_date(col(col_name), "yyyy-MM-dd"),
                    to_date(col(col_name), "MM/dd/yyyy"),
                    to_date(col(col_name), "dd-MM-yyyy")
                )
            )
        logger.info(f"Normalización de fechas completada para columnas: {date_columns}")
        return df
    
    def normalize_numbers(self, df, numeric_columns):
        """Estandariza números con punto decimal."""
        for col_name in numeric_columns:
            # Reemplaza comas por puntos y convierte a double
            df = df.withColumn(
                col_name,
                when(
                    isnull(col(col_name)), 
                    None
                ).otherwise(
                    regexp_replace(col(col_name), ",", ".").cast(DoubleType())
                )
            )
        logger.info(f"Normalización de números completada para columnas: {numeric_columns}")
        return df
    
    def normalize(self, hdfs_input_path, hdfs_output_path, date_cols, numeric_cols):
        """Ejecuta la normalización completa."""
        df = self.spark.read.csv(hdfs_input_path, header=True, inferSchema=True)
        logger.info(f"Datos cargados desde HDFS: {hdfs_input_path}")
        
        df = self.normalize_dates(df, date_cols)
        df = self.normalize_numbers(df, numeric_cols)
        
        df.write.mode("overwrite").csv(hdfs_output_path, header=True)
        logger.info(f"Datos normalizados guardados en HDFS: {hdfs_output_path}")
        
        return df