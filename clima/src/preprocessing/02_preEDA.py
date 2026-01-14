"""
Docstring for preprocessing.02_preEDA

1. Sacar estadísticos iniciales para evaluar el estado de los datos:
    Conteo de valores faltantes por columna y por fila.
    Minimos y máximos de las variables numéricas.
    Detección de valores atípicos.
2. Visualizar valores faltantes mediante gráficos sencillos con histogramas y Gráficos de pastel de:
    Canntidad de valores faltantes por columna.
    Cantidad de filas con 25, 50, 75 y 100 % de valores faltantes.
3.  Visualizaciones de distribución inicial de las variables numéricas:
    Histogramas.
    Boxplots.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, when, isnull, sum as spark_sum, 
    min as spark_min, max as spark_max, approx_percentile, 
    lit, round as spark_round
)
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class PreEDAAnalyzer:
    """Análisis exploratorio inicial de datos."""
    
    def __init__(self, spark: SparkSession, output_dir="hdfs:///preprocessing/reports"):
        self.spark = spark
        self.output_dir = output_dir
        
    def missing_stats_by_column(self, df):
        """Estadísticas de valores faltantes por columna."""
        total_rows = df.count()
        missing_stats = df.select([
            spark_sum(when(isnull(col(c)), 1).otherwise(0)).alias(c)
            for c in df.columns
        ]).collect()[0].asDict()
        
        stats_df = pd.DataFrame({
            'Columna': list(missing_stats.keys()),
            'Valores_Faltantes': list(missing_stats.values()),
            'Porcentaje': [round(100 * v / total_rows, 2) for v in missing_stats.values()]
        })
        stats_df = stats_df.sort_values('Valores_Faltantes', ascending=False)
        logger.info("Estadísticas de valores faltantes por columna:\n" + str(stats_df))
        return stats_df
    
    def missing_stats_by_row(self, df):
        """Estadísticas de valores faltantes por fila."""
        total_cols = len(df.columns)
        df_with_nulls = df.withColumn(
            'null_count',
            spark_sum(when(isnull(col(c)), 1).otherwise(0) for c in df.columns)
        )
        df_with_nulls = df_with_nulls.withColumn(
            'null_percentage',
            spark_round(100 * col('null_count') / total_cols, 2)
        )
        
        null_pct_dist = df_with_nulls.groupBy('null_percentage').count().collect()
        logger.info("Distribución de porcentaje de valores faltantes por fila:")
        for row in null_pct_dist:
            logger.info(f"  {row['null_percentage']}%: {row['count']} filas")
        
        return df_with_nulls
    
    def numeric_stats(self, df, numeric_cols):
        """Estadísticas de variables numéricas (min, max, percentiles)."""
        stats = {}
        for col_name in numeric_cols:
            col_stats = df.select(
                spark_min(col(col_name)).alias('min'),
                spark_max(col(col_name)).alias('max'),
                approx_percentile(col(col_name), 0.25).alias('q1'),
                approx_percentile(col(col_name), 0.50).alias('median'),
                approx_percentile(col(col_name), 0.75).alias('q3')
            ).collect()[0].asDict()
            stats[col_name] = col_stats
            
        logger.info("Estadísticas de variables numéricas:\n" + str(stats))
        return stats
    
    def detect_outliers_iqr(self, df, numeric_cols):
        """Detección de outliers usando IQR."""
        outlier_summary = {}
        for col_name in numeric_cols:
            stats = df.select(
                approx_percentile(col(col_name), 0.25).alias('q1'),
                approx_percentile(col(col_name), 0.75).alias('q3')
            ).collect()[0].asDict()
            
            q1, q3 = stats['q1'], stats['q3']
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            outlier_count = df.filter(
                (col(col_name) < lower_bound) | (col(col_name) > upper_bound)
            ).count()
            
            outlier_summary[col_name] = {
                'count': outlier_count,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound
            }
        
        logger.info("Outliers detectados (IQR):\n" + str(outlier_summary))
        return outlier_summary
    
    def plot_missing_by_column(self, stats_df, output_path):
        """Gráfico de pastel de valores faltantes por columna."""
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        
        # Gráfico de barras
        stats_df.plot(x='Columna', y='Valores_Faltantes', kind='bar', ax=axes[0])
        axes[0].set_title('Valores Faltantes por Columna')
        axes[0].set_ylabel('Cantidad')
        
        # Gráfico de pastel
        with_missing = stats_df[stats_df['Valores_Faltantes'] > 0]
        axes[1].pie(with_missing['Valores_Faltantes'], labels=with_missing['Columna'], autopct='%1.1f%%')
        axes[1].set_title('Distribución de Valores Faltantes')
        
        plt.tight_layout()
        plt.savefig(output_path + '/missing_by_column.png')
        logger.info(f"Gráfico guardado: {output_path}/missing_by_column.png")
    
    def plot_missing_by_row_distribution(self, df_with_nulls, output_path):
        """Distribución de filas por porcentaje de valores faltantes."""
        df_pandas = df_with_nulls.select('null_percentage').toPandas()
        
        fig, ax = plt.subplots(figsize=(10, 6))
        bins = [0, 25, 50, 75, 100]
        df_pandas['null_percentage'].hist(bins=bins, ax=ax, edgecolor='black')
        ax.set_xlabel('Porcentaje de Valores Faltantes (%)')
        ax.set_ylabel('Cantidad de Filas')
        ax.set_title('Distribución de Filas por % de Valores Faltantes')
        
        plt.tight_layout()
        plt.savefig(output_path + '/missing_by_row_distribution.png')
        logger.info(f"Gráfico guardado: {output_path}/missing_by_row_distribution.png")
    
    def plot_numeric_distributions(self, df, numeric_cols, output_path):
        """Histogramas y boxplots de variables numéricas."""
        df_pandas = df.select(numeric_cols).toPandas()
        
        # Histogramas
        fig, axes = plt.subplots(len(numeric_cols), 1, figsize=(10, 3*len(numeric_cols)))
        if len(numeric_cols) == 1:
            axes = [axes]
        
        for idx, col_name in enumerate(numeric_cols):
            df_pandas[col_name].hist(bins=30, ax=axes[idx], edgecolor='black')
            axes[idx].set_title(f'Histograma: {col_name}')
            axes[idx].set_xlabel('Valor')
            axes[idx].set_ylabel('Frecuencia')
        
        plt.tight_layout()
        plt.savefig(output_path + '/histograms.png')
        logger.info(f"Histogramas guardados: {output_path}/histograms.png")
        
        # Boxplots
        fig, axes = plt.subplots(1, len(numeric_cols), figsize=(5*len(numeric_cols), 5))
        if len(numeric_cols) == 1:
            axes = [axes]
        
        for idx, col_name in enumerate(numeric_cols):
            axes[idx].boxplot(df_pandas[col_name].dropna())
            axes[idx].set_title(f'Boxplot: {col_name}')
            axes[idx].set_ylabel('Valor')
        
        plt.tight_layout()
        plt.savefig(output_path + '/boxplots.png')
        logger.info(f"Boxplots guardados: {output_path}/boxplots.png")
    
    def analyze(self, hdfs_input_path, numeric_cols, output_dir=None):
        """Ejecuta análisis completo."""
        output_dir = output_dir or self.output_dir
        df = self.spark.read.csv(hdfs_input_path, header=True, inferSchema=True)
        logger.info(f"Datos cargados desde HDFS: {hdfs_input_path}")
        
        # Estadísticas
        stats_col = self.missing_stats_by_column(df)
        df_with_nulls = self.missing_stats_by_row(df)
        numeric_stats = self.numeric_stats(df, numeric_cols)
        outliers = self.detect_outliers_iqr(df, numeric_cols)
        
        # Visualizaciones
        self.plot_missing_by_column(stats_col, output_dir)
        self.plot_missing_by_row_distribution(df_with_nulls, output_dir)
        self.plot_numeric_distributions(df, numeric_cols, output_dir)
        
        logger.info(f"Análisis completado. Reportes guardados en: {output_dir}")
        return {
            'missing_by_column': stats_col,
            'numeric_stats': numeric_stats,
            'outliers': outliers
        }