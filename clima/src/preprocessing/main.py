"""
Docstring for preprocessing.main
Se orquestan las 4 fases del prepocesamiento.
interactúa con los chunks almacenados en Hadoop HDFS, una vez preprocesado se 
actualizan los datos almacenados
"""
from pyspark.sql import SparkSession
import logging
import sys
from pathlib import Path

# Importar módulos de preprocesamiento
from preprocessing.normalize import DataNormalizer
from preprocessing.preEDA import PreEDAAnalyzer
from preprocessing.clean import DataCleaner
from preprocessing.knn import KNNImputer

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('preprocessing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PreprocessingPipeline:
    """Pipeline completo de preprocesamiento con 4 fases."""
    
    def __init__(self, hdfs_raw_path="hdfs:///data/raw", 
                 hdfs_output_base="hdfs:///data/processed"):
        """
        Inicializa el pipeline.
        
        Args:
            hdfs_raw_path: Ruta HDFS de datos crudos
            hdfs_output_base: Ruta base HDFS para datos procesados
        """
        self.spark = SparkSession.builder \
            .appName("ClimateDataPreprocessing") \
            .getOrCreate()
        
        self.hdfs_raw_path = hdfs_raw_path
        self.hdfs_output_base = hdfs_output_base
        self.stage_paths = {
            'raw': hdfs_raw_path,
            'normalized': f"{hdfs_output_base}/01_normalized",
            'analyzed': f"{hdfs_output_base}/02_analyzed",
            'cleaned': f"{hdfs_output_base}/03_cleaned",
            'imputed': f"{hdfs_output_base}/04_imputed"
        }
        
        logger.info("Pipeline de preprocesamiento inicializado")
    
    def stage_01_normalize(self, date_columns, numeric_columns):
        """
        FASE 1: Normalización de formatos.
        - Estandariza fechas a dd/mm/yyyy
        - Estandariza números con punto decimal
        """
        logger.info("=" * 60)
        logger.info("FASE 1: NORMALIZACIÓN DE DATOS")
        logger.info("=" * 60)
        
        normalizer = DataNormalizer(self.spark)
        df_normalized = normalizer.normalize(
            hdfs_input_path=self.stage_paths['raw'],
            hdfs_output_path=self.stage_paths['normalized'],
            date_cols=date_columns,
            numeric_cols=numeric_columns
        )
        
        logger.info("✓ Fase 1 completada")
        return df_normalized
    
    def stage_02_eda(self, numeric_columns):
        """
        FASE 2: Análisis Exploratorio Inicial.
        - Genera estadísticas de valores faltantes
        - Detecta outliers
        - Crea visualizaciones
        """
        logger.info("=" * 60)
        logger.info("FASE 2: ANÁLISIS EXPLORATORIO (EDA)")
        logger.info("=" * 60)
        
        analyzer = PreEDAAnalyzer(self.spark, output_dir=self.stage_paths['analyzed'])
        analysis_results = analyzer.analyze(
            hdfs_input_path=self.stage_paths['normalized'],
            numeric_cols=numeric_columns,
            output_dir=self.stage_paths['analyzed']
        )
        
        logger.info("✓ Fase 2 completada")
        logger.info(f"  Reportes guardados en: {self.stage_paths['analyzed']}")
        return analysis_results
    
    def stage_03_clean(self, numeric_columns, null_threshold=0.5):
        """
        FASE 3: Limpieza de Datos.
        - Reemplaza outliers extremos con NULL
        - Elimina filas con >50% de valores NULL
        """
        logger.info("=" * 60)
        logger.info("FASE 3: LIMPIEZA DE DATOS")
        logger.info("=" * 60)
        
        cleaner = DataCleaner(self.spark, iqr_multiplier=1.5)
        df_cleaned = cleaner.clean(
            hdfs_input_path=self.stage_paths['normalized'],
            hdfs_output_path=self.stage_paths['cleaned'],
            numeric_cols=numeric_columns,
            null_threshold=null_threshold
        )
        
        logger.info("✓ Fase 3 completada")
        return df_cleaned
    
    def stage_04_imputation(self, numeric_columns, k=5, target_cols=None):
        """
        FASE 4: Imputación de Valores Faltantes con KNN.
        - Selecciona características relevantes
        - Imputa valores usando k-NN
        """
        logger.info("=" * 60)
        logger.info("FASE 4: IMPUTACIÓN CON K-NEAREST NEIGHBORS")
        logger.info("=" * 60)
        
        imputer = KNNImputer(self.spark, k=k)
        df_imputed = imputer.impute(
            hdfs_input_path=self.stage_paths['cleaned'],
            hdfs_output_path=self.stage_paths['imputed'],
            numeric_cols=numeric_columns,
            target_cols=target_cols
        )
        
        logger.info("✓ Fase 4 completada")
        return df_imputed
    
    def run(self, date_columns, numeric_columns, null_threshold=0.5, k=5):
        """
        Ejecuta el pipeline completo de preprocesamiento.
        
        Args:
            date_columns: Lista de columnas con fechas
            numeric_columns: Lista de columnas numéricas
            null_threshold: Umbral de nulos para eliminar filas (default 0.5 = 50%)
            k: Número de vecinos para KNN (default 5)
        """
        try:
            logger.info("╔" + "=" * 58 + "╗")
            logger.info("║" + " PIPELINE DE PREPROCESAMIENTO - DATOS CLIMÁTICOS ".center(58) + "║")
            logger.info("╚" + "=" * 58 + "╝")
            
            # Fase 1: Normalización
            self.stage_01_normalize(date_columns, numeric_columns)
            
            # Fase 2: EDA
            self.stage_02_eda(numeric_columns)
            
            # Fase 3: Limpieza
            self.stage_03_clean(numeric_columns, null_threshold)
            
            # Fase 4: Imputación
            self.stage_04_imputation(numeric_columns, k=k)
            
            logger.info("\n" + "=" * 60)
            logger.info("✓ PREPROCESAMIENTO COMPLETADO EXITOSAMENTE")
            logger.info("=" * 60)
            logger.info(f"Datos finales guardados en: {self.stage_paths['imputed']}")
            
        except Exception as e:
            logger.error(f"Error durante el preprocesamiento: {str(e)}", exc_info=True)
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    # Configuración
    DATE_COLUMNS = ['fecha']  # Ajustar según tu dataset
    NUMERIC_COLUMNS = ['temperatura', 'humedad', 'presion', 'velocidad_viento']  # Ajustar según tu dataset
    
    # Crear pipeline
    pipeline = PreprocessingPipeline(
        hdfs_raw_path="hdfs:///data/raw/clima",
        hdfs_output_base="hdfs:///data/processed/clima"
    )
    
    # Ejecutar
    pipeline.run(
        date_columns=DATE_COLUMNS,
        numeric_columns=NUMERIC_COLUMNS,
        null_threshold=0.5,  # Eliminar filas con >50% nulos
        k=5  # Usar 5 vecinos en KNN
    )