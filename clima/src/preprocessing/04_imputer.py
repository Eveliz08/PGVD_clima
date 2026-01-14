"""
Docstring for preprocessing.04_KNN

Con el algoritmo KNN (K-Nearest Neighbors) se imputan los valores faltantes en el conjunto de datos.
1. Seleccionar las características relevantes para la imputación por cada variable con valores faltantes.
2. Determinar el valor de K (número de vecinos) óptimo mediante validación cruzada.
3. Deterimar el criterio a utilizar: moda, media, distancia, etc por cada variable.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, isnull, avg, lit, row_number, 
    sum as spark_sum, sqrt, abs as spark_abs, 
    min as spark_min, max as spark_max, approx_percentile
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.linalg import Vectors
from statistics import mode, StatisticsError
import logging

logger = logging.getLogger(__name__)

class KNNImputer:
    """Imputa valores faltantes usando K-Nearest Neighbors."""
    
    def __init__(self, spark: SparkSession, k=5):
        self.spark = spark
        self.k = k
    
    def is_continuous_variable(self, df, col_name):
        """Determina si una variable es continua o categórica."""
        sample = df.select(col(col_name)).filter(~isnull(col(col_name))).limit(1000)
        
        if sample.count() == 0:
            return True
        
        first_val = sample.collect()[0][0]
        is_numeric = isinstance(first_val, (int, float))
        
        if is_numeric:
            return True
        else:
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
            stats = df.select(
                spark_min(col(col_name)).alias('min_val'),
                spark_max(col(col_name)).alias('max_val')
            ).collect()[0].asDict()
            
            min_val, max_val = stats['min_val'], stats['max_val']
            if min_val is not None and max_val is not None and min_val != max_val:
                df_normalized = df_normalized.withColumn(
                    col_name + '_norm',
                    (col(col_name) - min_val) / (max_val - min_val)
                )
        
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
        """Imputa una columna específica usando KNN."""
        logger.info(f"Imputando valores en columna: {target_col}")
        
        feature_cols = self.select_features_for_imputation(df, target_col, numeric_cols)
        
        if not feature_cols:
            logger.warning(f"No hay características disponibles para imputar {target_col}")
            return df
        
        # Determinar si la variable es continua o categórica
        is_continuous = self.is_continuous_variable(df, target_col)
        logger.info(f"Variable {target_col} es {'continua' if is_continuous else 'categórica'}")
        
        # Normalizar características
        df_normalized = self.normalize_features(df, feature_cols)
        
        # Separar filas con y sin valores faltantes
        df_missing = df_normalized.filter(isnull(col(target_col)))
        df_complete = df_normalized.filter(~isnull(col(target_col)))
        
        if df_missing.count() == 0:
            logger.info(f"No hay valores faltantes en {target_col}")
            return df.select([c for c in df.columns if not c.endswith('_norm')])
        
        # Para cada fila con valor faltante
        imputed_rows = []
        for missing_row in df_missing.collect():
            # Calcular distancias a todas las filas completas
            distances = []
            for complete_row in df_complete.collect():
                dist = self.calculate_euclidean_distance(
                    missing_row.asDict(), 
                    complete_row.asDict(), 
                    feature_cols
                )
                distances.append((dist, complete_row[target_col]))
            
            # Obtener k vecinos más cercanos
            distances.sort(key=lambda x: x[0])
            k_neighbors = distances[:min(self.k, len(distances))]
            
            # Imputar usando mediana (continua) o moda (categórica)
            neighbor_values = [val for _, val in k_neighbors]
            
            if is_continuous:
                imputed_value = self.calculate_median(neighbor_values)
                logger.debug(f"Valor imputado (mediana) para {target_col}: {imputed_value}")
            else:
                imputed_value = self.calculate_mode(neighbor_values)
                logger.debug(f"Valor imputado (moda) para {target_col}: {imputed_value}")
            
            # Actualizar fila
            row_dict = missing_row.asDict()
            row_dict[target_col] = imputed_value
            imputed_rows.append(row_dict)
        
        # Reconstruir dataframe
        if imputed_rows:
            from pyspark.sql import Row
            imputed_df = self.spark.createDataFrame(
                [Row(**row) for row in imputed_rows]
            )
            df_result = df_complete.unionByName(imputed_df, allowMissingColumns=True)
        else:
            df_result = df_complete
        
        # Eliminar columnas normalizadas
        norm_cols = [c for c in df_result.columns if c.endswith('_norm')]
        df_result = df_result.drop(*norm_cols)
        
        logger.info(f"Imputación completada para {target_col}")
        return df_result
    
    def impute(self, hdfs_input_path, hdfs_output_path, numeric_cols, target_cols=None):
        """Ejecuta imputación KNN para todas las columnas con valores faltantes."""
        df = self.spark.read.csv(hdfs_input_path, header=True, inferSchema=True)
        logger.info(f"Datos cargados desde HDFS: {hdfs_input_path}")
        
        # Si no se especifican columnas objetivo, procesar todas
        if target_cols is None:
            target_cols = numeric_cols
        
        # Imputar cada columna
        for col_name in target_cols:
            missing_count = df.filter(isnull(col(col_name))).count()
            if missing_count > 0:
                df = self.impute_column(df, col_name, numeric_cols)
            else:
                logger.info(f"No hay valores faltantes en {col_name}")
        
        df.write.mode("overwrite").csv(hdfs_output_path, header=True)
        logger.info(f"Datos imputados guardados en HDFS: {hdfs_output_path}")
        
        return df