"""
Docstring for preprocessing.03_clean

1. Eliminar (colocar NULL)  valores atípicos extremos dados por una fórmula como posibles errores.
2. Eliminar filas que tengas mas del 50% de sus datos con NULL.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnull, approx_percentile, 
    sum as spark_sum, round as spark_round, lit
)
import logging

logger = logging.getLogger(__name__)

class DataCleaner:
    """Limpia valores atípicos extremos y filas con muchos nulos."""
    
    def __init__(self, spark: SparkSession, iqr_multiplier=1.5):
        self.spark = spark
        self.iqr_multiplier = iqr_multiplier  # Multiplicador para definir outliers extremos
        
    def replace_outliers_with_null(self, df, numeric_cols):
        """Reemplaza outliers extremos (fuera de 1.5*IQR) con NULL."""
        logger.info(f"Reemplazando outliers extremos en columnas: {numeric_cols}")
        
        for col_name in numeric_cols:
            # Calcular Q1, Q3 e IQR
            stats = df.select(
                approx_percentile(col(col_name), 0.25).alias('q1'),
                approx_percentile(col(col_name), 0.75).alias('q3')
            ).collect()[0].asDict()
            
            q1, q3 = stats['q1'], stats['q3']
            if q1 is None or q3 is None:
                logger.warning(f"No se pudo calcular IQR para {col_name}, omitiendo")
                continue
            
            iqr = q3 - q1
            lower_bound = q1 - self.iqr_multiplier * iqr
            upper_bound = q3 + self.iqr_multiplier * iqr
            
            # Reemplazar outliers con NULL
            df = df.withColumn(
                col_name,
                when(
                    (col(col_name) < lower_bound) | (col(col_name) > upper_bound),
                    None
                ).otherwise(col(col_name))
            )
            
            logger.info(f"  {col_name}: límites [{lower_bound:.2f}, {upper_bound:.2f}]")
        
        return df
    
    def remove_rows_with_excessive_nulls(self, df, null_threshold=0.5):
        """Elimina filas con más del threshold% de valores NULL."""
        total_cols = len(df.columns)
        
        # Contar valores nulos por fila
        df_with_null_count = df.withColumn(
            '__null_count__',
            spark_sum(when(isnull(col(c)), 1).otherwise(0) for c in df.columns)
        )
        
        # Contar valores nulos permitidos
        max_nulls = int(total_cols * null_threshold)
        
        # Filtrar filas
        df_cleaned = df_with_null_count.filter(col('__null_count__') <= max_nulls).drop('__null_count__')
        
        removed_rows = df.count() - df_cleaned.count()
        logger.info(f"Filas eliminadas por exceso de valores NULL (threshold={null_threshold*100}%): {removed_rows}")
        
        return df_cleaned
    
    def clean(self, hdfs_input_path, hdfs_output_path, numeric_cols, null_threshold=0.5):
        """Ejecuta limpieza completa."""
        df = self.spark.read.csv(hdfs_input_path, header=True, inferSchema=True)
        logger.info(f"Datos cargados desde HDFS: {hdfs_input_path}")
        
        initial_count = df.count()
        
        # Paso 1: Reemplazar outliers con NULL
        df = self.replace_outliers_with_null(df, numeric_cols)
        
        # Paso 2: Eliminar filas con muchos nulos
        df = self.remove_rows_with_excessive_nulls(df, null_threshold)
        
        final_count = df.count()
        logger.info(f"Limpieza completada: {initial_count} -> {final_count} filas ({final_count - initial_count} eliminadas)")
        
        df.write.mode("overwrite").csv(hdfs_output_path, header=True)
        logger.info(f"Datos limpios guardados en HDFS: {hdfs_output_path}")
        
        return df