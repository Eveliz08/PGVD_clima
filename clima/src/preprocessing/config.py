"""
Configuración centralizada para el Pipeline de Clima
Modifica estos valores para personalizar el comportamiento
"""

# ============================================
# CONFIGURACIÓN DE HDFS
# ============================================

HDFS_NAMENODE = "namenode:9000"
HDFS_RAW_PATH = "hdfs://namenode:9000/data/raw"
HDFS_PROCESSED_BASE = "hdfs://namenode:9000/data/processed"
HDFS_RESULTS_PATH = "hdfs://namenode:9000/data/results"

# ============================================
# CONFIGURACIÓN DE SPARK
# ============================================

SPARK_MASTER = "spark://spark-master:7077"
SPARK_APP_NAME = "ClimateDataPreprocessing"

# Memoria para Driver y Executors (en GB)
# Aumentar si tienes datos muy grandes
SPARK_DRIVER_MEMORY = "2g"
SPARK_EXECUTOR_MEMORY = "2g"
SPARK_EXECUTOR_CORES = "2"
SPARK_NUM_EXECUTORS = "2"

# ============================================
# CONFIGURACIÓN DE DATOS
# ============================================

# Archivo CSV a procesar
CSV_INPUT_FILE = "GlobalLandTemperaturesByCity.csv"
CSV_LOCAL_PATH = "/app/clima/data/GlobalLandTemperaturesByCity.csv"

# Columnas de fecha para normalizar
DATE_COLUMNS = ["dt"]

# Columnas numéricas para normalizar
NUMERIC_COLUMNS = [
    "AverageTemperature",
    "AverageTemperatureUncertainty"
]

# ============================================
# CONFIGURACIÓN DE HIVE (OPCIONAL)
# ============================================

HIVE_ENABLED = False  # Cambiar a True si quieres guardar en Hive
HIVE_DATABASE = "climate_db"
HIVE_TABLE = "temperature_data"
HIVE_SERVER = "hive-server:10000"

# ============================================
# CONFIGURACIÓN DE LOGGING
# ============================================

LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR
LOG_FILE = "/app/logs/preprocessing.log"

# ============================================
# CONFIGURACIÓN DEL PIPELINE
# ============================================

# Fases a ejecutar (True = ejecutar, False = saltar)
PHASE_NORMALIZE = True
PHASE_EDA = True
PHASE_CLEAN = True
PHASE_KNN_IMPUTE = True

# Parámetros de limpieza
# Eliminar filas donde se pierdan más del X% de datos
DROP_MISSING_THRESHOLD = 0.5

# Parámetros de KNN Imputation
KNN_K = 5
KNN_METRIC = "euclidean"

# ============================================
# CONFIGURACIÓN DE OUTPUT
# ============================================

# Formato de salida (parquet, csv, orc)
OUTPUT_FORMAT = "parquet"

# Particionamiento
PARTITION_BY = None  # Ejemplo: "year" o None para sin particiones
