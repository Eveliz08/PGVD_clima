"""
Docstring for preprocessing.04_KNN

Con el algoritmo KNN (K-Nearest Neighbors) se imputan los valores faltantes en el conjunto de datos.
1. Seleccionar las características relevantes para la imputación por cada variable con valores faltantes.
2. Determinar el valor de K (número de vecinos) óptimo mediante validación cruzada.
3. Deterimar el criterio a utilizar: moda, media, distancia, etc por cada variable.
"""
from pyspark.sql import SparkSession, Window, Row
from pyspark.sql.functions import (
    col, when, isnull, avg, lit, row_number, 
    sum as spark_sum, sqrt, abs as spark_abs, 
    min as spark_min, max as spark_max, approx_percentile,
    monotonically_increasing_id, collect_list, udf
)
from pyspark.sql.types import DoubleType, StringType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.linalg import Vectors
from statistics import mode, StatisticsError
import logging

logger = logging.getLogger(__name__)

# Standalone functions for UDFs (outside the class to avoid pickling issues)
def _median_from_list(arr):
    """Compute median from a Python list, ignoring None."""
    if arr is None:
        return None
    vals = [v for v in arr if v is not None]
    if not vals:
        return None
    vals_sorted = sorted(vals)
    n = len(vals_sorted)
    if n % 2 == 0:
        return float((vals_sorted[n // 2 - 1] + vals_sorted[n // 2]) / 2)
    else:
        return float(vals_sorted[n // 2])

def _mode_from_list(arr):
    """Compute mode from a Python list, ignoring None; returns string."""
    if arr is None:
        return None
    vals = [v for v in arr if v is not None]
    if not vals:
        return None
    try:
        return str(mode(vals))
    except StatisticsError:
        from collections import Counter
        return str(Counter(vals).most_common(1)[0][0])

class KNNImputer:
    """Imputa valores faltantes usando K-Nearest Neighbors."""
    
    def __init__(self, spark: SparkSession, k=5):
        self.spark = spark
        self.k = k
    
    def is_continuous_variable(self, df, col_name):
        """Determina si una variable es continua o categórica (a partir de tipo de dato)."""
        # Preferir tipo spark: numeric types -> continua
        dt = dict(df.dtypes).get(col_name, "").lower()
        if any(x in dt for x in ["int", "double", "float", "long", "decimal"]):
            return True
        # si tipo no numérico, tratar como categórica
        return False
    
    def calculate_median(self, values):
        """Calcula la mediana de una lista de valores."""
        sorted_values = sorted([v for v in values if v is not None])
        if not sorted_values:
            return None
        n = len(sorted_values)
        if n % 2 == 0:
            return (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
        else:
            return sorted_values[n // 2]
    
    def calculate_mode(self, values):
        """Calcula la moda de una lista de valores."""
        valid_values = [v for v in values if v is not None]
        if not valid_values:
            return None
        try:
            return mode(valid_values)
        except StatisticsError:
            # Si no hay moda única, retorna el valor más frecuente
            from collections import Counter
            return Counter(valid_values).most_common(1)[0][0]
        
    def select_features_for_imputation(self, df, target_col, numeric_cols):
        """
        Selecciona características relevantes para imputar una variable.
        Retorna columnas numéricas excluyendo la columna objetivo.
        """
        features = [c for c in numeric_cols if c != target_col]
        logger.info(f"Características seleccionadas para imputar {target_col}: {features}")
        return features
    
    def normalize_features(self, df, feature_cols):
        """Normaliza características numéricas al rango [0, 1]."""
        df_normalized = df
        for col_name in feature_cols:
            # Validar columna: intentar directamente obtener min/max y capturar cualquier error
            try:
                stats = df.select(
                    spark_min(col(col_name)).alias('min_val'),
                    spark_max(col(col_name)).alias('max_val')
                ).collect()[0].asDict()
            except Exception as e:
                logger.warning(f"Error al procesar columna {col_name} (posiblemente sin valores válidos o tipo incompatible): {e}; omitiendo normalización.")
                continue
            
            min_val, max_val = stats.get('min_val'), stats.get('max_val')
            if min_val is None or max_val is None:
                logger.warning(f"Columna {col_name} tiene min/max nulo, omitiendo normalización.")
                continue
            if min_val == max_val:
                logger.warning(f"Columna {col_name} no tiene variación (min==max={min_val}), omitiendo normalización.")
                continue

            # Normalizar la columna
            try:
                df_normalized = df_normalized.withColumn(
                    col_name + '_norm',
                    (col(col_name) - min_val) / (max_val - min_val)
                )
            except Exception as e:
                logger.warning(f"Error al normalizar columna {col_name}: {e}; omitiendo normalización de esta columna.")
                continue
        
        return df_normalized
    
    def calculate_euclidean_distance(self, row_values, neighbor_values, feature_cols):
        """Calcula distancia euclidiana entre dos filas."""
        distance = 0
        for feat in feature_cols:
            val1 = row_values.get(feat + '_norm', 0)
            val2 = neighbor_values.get(feat + '_norm', 0)
            if val1 is not None and val2 is not None:
                distance += (val1 - val2) ** 2
        return distance ** 0.5
    
    def impute_column(self, df, target_col, numeric_cols):
        """Imputa una columna específica usando KNN (implementación basada en DataFrame)."""
        logger.info(f"Imputando valores en columna: {target_col}")
        
        feature_cols = self.select_features_for_imputation(df, target_col, numeric_cols)
        if not feature_cols:
            logger.warning(f"No hay características disponibles para imputar {target_col}")
            return df

        # Determinar si la variable es continua o categórica
        is_continuous = self.is_continuous_variable(df, target_col)
        logger.info(f"Variable {target_col} es {'continua' if is_continuous else 'categórica'}")
        
        # Normalizar características (añade columnas <feat>_norm)
        df_normalized = self.normalize_features(df, feature_cols)
        
        # Preparar DataFrames con ids para emparejamiento
        df_complete = df_normalized.filter(~isnull(col(target_col))).withColumn("__cid__", monotonically_increasing_id())
        df_missing = df_normalized.filter(isnull(col(target_col))).withColumn("__mid__", monotonically_increasing_id())
        
        # Si no hay filas faltantes, devolver df original sin columnas _norm
        if df_missing.count() == 0:
            norm_cols = [c for c in df.columns if c.endswith('_norm')]
            return df.drop(*norm_cols) if norm_cols else df

        # Build list of normalized feature column names
        norm_feats = [f + '_norm' for f in feature_cols if f + '_norm' in df_normalized.columns]
        if not norm_feats:
            # Fallback: simple global median/mode imputation
            orig_cols = df.columns
            if is_continuous:
                try:
                    median_val = df_complete.approxQuantile(target_col, [0.5], 0.0)[0]
                except Exception:
                    median_val = None
                df_missing_filled = df_missing.withColumn(target_col, lit(median_val))
            else:
                mode_row = df_complete.groupBy(target_col).count().orderBy(col("count").desc()).limit(1).collect()
                mode_val = mode_row[0][target_col] if mode_row else None
                df_missing_filled = df_missing.withColumn(target_col, lit(mode_val))

            for c in orig_cols:
                if c not in df_missing_filled.columns:
                    df_missing_filled = df_missing_filled.withColumn(c, lit(None))
            df_missing_filled = df_missing_filled.select(*orig_cols)

            df_result = df_complete.select(*orig_cols).unionByName(df_missing_filled.select(*orig_cols), allowMissingColumns=True)
            logger.info(f"Simple imputation applied for {target_col}")
            return df_result

        # build sum of squared diffs
        sq_expr = None
        for nf in norm_feats:
            expr_part = (col(f"m.{nf}") - col(f"c.{nf}"))**2
            sq_expr = expr_part if sq_expr is None else (sq_expr + expr_part)
        dist_expr = sqrt(sq_expr).alias("distance")

        # Cross-join y calcular distancia
        pairs = df_missing.alias("m").crossJoin(df_complete.alias("c"))
        pairs = pairs.withColumn("distance", dist_expr)

        # k vecinos más cercanos
        w = Window.partitionBy(col("m.__mid__")).orderBy(col("distance"))
        pairs_topk = pairs.withColumn("rn", row_number().over(w)).filter(col("rn") <= self.k)

        # Agregar lista de valores vecinos
        neighbors = pairs_topk.select(col("m.__mid__").alias("__mid__"), col(f"c.{target_col}").alias("neighbor")).groupBy("__mid__").agg(collect_list("neighbor").alias("neighbors"))

        # Crear UDFs usando funciones standalone (evitar referencia a self)
        if is_continuous:
            median_udf_func = udf(_median_from_list, DoubleType())
            neighbors = neighbors.withColumn("imputed", median_udf_func(col("neighbors")))
        else:
            mode_udf_func = udf(_mode_from_list, StringType())
            neighbors = neighbors.withColumn("imputed", mode_udf_func(col("neighbors")))

        # Unir valores imputados
        df_missing_pref = df_missing.alias("m")
        missing_with_imputed = df_missing_pref.join(neighbors, on="__mid__", how="left")

        # Reemplazar target_col por imputed
        if is_continuous:
            filled_missing = missing_with_imputed.withColumn(target_col, col("imputed").cast(DoubleType())).drop("imputed")
        else:
            filled_missing = missing_with_imputed.withColumn(target_col, col("imputed")).drop("imputed")

        # Seleccionar columnas originales
        orig_cols = df.columns
        for c in orig_cols:
            if c not in filled_missing.columns:
                filled_missing = filled_missing.withColumn(c, lit(None))
        filled_missing = filled_missing.select(*orig_cols)

        # Unión con filas completas
        df_result = df_complete.select(*orig_cols).unionByName(filled_missing.select(*orig_cols), allowMissingColumns=True)

        # Eliminar columnas normalizadas
        norm_cols_final = [c for c in df_result.columns if c.endswith("_norm")]
        if norm_cols_final:
            df_result = df_result.drop(*norm_cols_final)

        logger.info(f"Imputación completada para {target_col}")
        return df_result
    
    def impute(self, df, numeric_cols=None, target_cols=None, write_output_path=None):
        """
        Ejecuta imputación KNN sobre un DataFrame de Spark.

        Args:
            df: Spark DataFrame (ya cargado).
            numeric_cols: lista opcional de columnas numéricas/continuas. Si None, se detectan automáticamente.
            target_cols: lista opcional de columnas a imputar. Si None, se imputan las columnas con valores faltantes.
            write_output_path: ruta HDFS opcional para escribir el resultado (si se desea).
        
        Returns:
            df_imputed: Spark DataFrame imputado.
        """
        # detectar columnas numéricas si no se pasaron
        if numeric_cols is None:
            numeric_cols = [c for c, t in df.dtypes if any(x in t for x in ['int', 'double', 'float', 'long', 'decimal'])]
        
        # determinar columnas objetivo si no se pasaron (columnas con nulos)
        if target_cols is None:
            target_cols = []
            for c in df.columns:
                if df.filter(isnull(col(c))).limit(1).count() > 0:
                    target_cols.append(c)
        
        logger.info(f"Columnas numéricas detectadas: {numeric_cols}")
        logger.info(f"Columnas objetivo para imputación: {target_cols}")
        
        df_current = df
        for col_name in target_cols:
            df_current = self.impute_column(df_current, col_name, numeric_cols)
        
        # opcional: escribir a HDFS si se especificó ruta de salida
        if write_output_path:
            try:
                df_current.write.mode("overwrite").option("header", "true").csv(write_output_path)
                logger.info(f"Datos imputados guardados en HDFS: {write_output_path}")
            except Exception as e:
                logger.error(f"Error al escribir imputación en HDFS: {e}")
        
        return df_current