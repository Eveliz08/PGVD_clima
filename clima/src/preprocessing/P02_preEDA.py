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
from functools import reduce
import operator

logger = logging.getLogger(__name__)

class PreEDAAnalyzer:
    """Análisis exploratorio inicial de datos."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
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
        # construir expresión para contar nulos por fila
        null_exprs = [when(isnull(col(c)), 1).otherwise(0) for c in df.columns]
        null_count_expr = reduce(operator.add, null_exprs)
        df_with_nulls = df.withColumn('null_count', null_count_expr)
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
    
    def plot_missing_by_column(self, stats_df):
        """Gráfico de barras y pastel de valores faltantes por columna. Devuelve figura."""
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        
        # Gráfico de barras
        stats_df.plot(x='Columna', y='Valores_Faltantes', kind='bar', ax=axes[0], color='skyblue')
        axes[0].set_title('Valores Faltantes por Columna')
        axes[0].set_ylabel('Cantidad')
        
        # Gráfico de pastel (solo columnas con >0)
        with_missing = stats_df[stats_df['Valores_Faltantes'] > 0]
        if not with_missing.empty:
            axes[1].pie(with_missing['Valores_Faltantes'], labels=with_missing['Columna'], autopct='%1.1f%%')
            axes[1].set_title('Distribución de Valores Faltantes')
        else:
            axes[1].text(0.5, 0.5, 'No missing values', ha='center', va='center')
            axes[1].set_title('Distribución de Valores Faltantes')
        
        plt.tight_layout()
        # No guardar en disco, devolver figura
        return fig

    def plot_missing_by_row_distribution(self, df_with_nulls):
        """Distribución de filas por porcentaje de valores faltantes. Devuelve figura."""
        df_pandas = df_with_nulls.select('null_percentage').toPandas()
        
        fig, ax = plt.subplots(figsize=(10, 6))
        bins = [0, 25, 50, 75, 100]
        df_pandas['null_percentage'].hist(bins=bins, ax=ax, edgecolor='black')
        ax.set_xlabel('Porcentaje de Valores Faltantes (%)')
        ax.set_ylabel('Cantidad de Filas')
        ax.set_title('Distribución de Filas por % de Valores Faltantes')
        
        plt.tight_layout()
        # No guardar en disco, devolver figura
        return fig

    def plot_numeric_distributions(self, df, numeric_cols, sample_fraction=0.1):
        """Histogramas y boxplots de variables numéricas. Devuelve lista de figuras."""
        plots = []
        # sample para rendimiento
        df_sample = df.select(numeric_cols).sample(False, sample_fraction).toPandas()
        # Histograma combinado (una figura por variable)
        for col_name in numeric_cols:
            fig, ax = plt.subplots(figsize=(8, 4))
            df_sample[col_name].dropna().hist(bins=50, ax=ax, color='lightgreen', edgecolor='black')
            ax.set_title(f'Histograma: {col_name}')
            ax.set_xlabel('Valor')
            ax.set_ylabel('Frecuencia')
            plt.tight_layout()
            # No guardar en disco, devolver figura
            plots.append(fig)
            
            fig2, ax2 = plt.subplots(figsize=(6, 4))
            sns.boxplot(data=df_sample, y=col_name, ax=ax2, color='lightcoral')
            ax2.set_title(f'Boxplot: {col_name}')
            ax2.set_ylabel('Valor')
            plt.tight_layout()
            # No guardar en disco, devolver figura
            plots.append(fig2)
        
        return plots

    def analyze(self, hdfs_input_path, numeric_cols=None):
        """Ejecuta análisis completo y devuelve (stats_df, plots)."""
        df = self.spark.read.csv(hdfs_input_path, header=True, inferSchema=True)
        logger.info(f"Datos cargados desde HDFS: {hdfs_input_path}")
        
        # detectar columnas numéricas si no se pasaron
        if numeric_cols is None:
            numeric_cols = [c for c, t in df.dtypes if t in ['int', 'bigint', 'double', 'float']]

        # Estadísticas
        stats_col = self.missing_stats_by_column(df)  # pandas DF with Columna, Valores_Faltantes, Porcentaje
        df_with_nulls = self.missing_stats_by_row(df)
        numeric_stats = self.numeric_stats(df, numeric_cols)  # dict per col
        outliers = self.detect_outliers_iqr(df, numeric_cols)  # dict per col
        
        # Construir tabla final por variable: missing, outliers, min, max
        rows = []
        for col_name in numeric_cols:
            missing_count = int(stats_col.loc[stats_col['Columna'] == col_name, 'Valores_Faltantes'].values[0]) if col_name in stats_col['Columna'].values else 0
            outlier_count = int(outliers.get(col_name, {}).get('count', 0))
            min_val = numeric_stats.get(col_name, {}).get('min')
            max_val = numeric_stats.get(col_name, {}).get('max')
            rows.append({
                'variable': col_name,
                'missing': missing_count,
                'outliers': outlier_count,
                'min': min_val,
                'max': max_val
            })
        stats_df = pd.DataFrame(rows)
        
        # Visualizaciones: devolver Figuras en una lista (NO GUARDAR EN DISCO)
        plots = []
        # missing by column figure
        fig_missing_col = self.plot_missing_by_column(stats_col)
        plots.append(fig_missing_col)
        # missing by row distribution
        fig_missing_row = self.plot_missing_by_row_distribution(df_with_nulls)
        plots.append(fig_missing_row)
        # numeric distributions (histograms + boxplots)
        numeric_plots = self.plot_numeric_distributions(df, numeric_cols)
        plots.extend(numeric_plots)
        
        logger.info("Análisis completado (figuras en memoria).")
        return stats_df, plots