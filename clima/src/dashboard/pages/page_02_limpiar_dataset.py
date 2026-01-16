import tempfile
import shutil
import streamlit as st
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.utils import IllegalArgumentException
from py4j.protocol import Py4JJavaError
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
# IMPORTACIONES A MODO ABSOLUTO (antes había imports relativos que provocaban el error)
from tools.download_data import check_file_exists_in_hdfs, run_command, verify_hdfs_upload, upload_to_hdfs, HDFS_PATH, FILE_NAME
from preprocessing.P01_normalize import DataNormalizer
from preprocessing.P02_preEDA import PreEDAAnalyzer
from preprocessing.P03_clean import DataCleaner
from preprocessing.P04_imputer import KNNImputer
from matplotlib.figure import Figure
import plotly.graph_objs as go

def show(spark = None):
    """Página para limpiar y procesar el dataset."""
    
    st.title("🧹 Limpiar Dataset")
    
    exists, _ = check_file_exists_in_hdfs()
    if not exists:
        st.warning("⚠️ Por favor, carga un dataset primero en el módulo 'Cargar Dataset'")
        return
    
    # Inicializar Spark si no existe
    if 'spark' not in st.session_state:
        st.session_state.spark = SparkSession.builder.appName("ClimaDashboard").getOrCreate()
    
    spark = st.session_state.spark
    
    # Cargar datos desde HDFS si no están en sesión
    if 'df_spark' not in st.session_state:
        with st.spinner("Cargando datos desde HDFS..."):
            try:
                df_spark = spark.read.csv("hdfs://namenode:9000/clima/GlobalLandTemperaturesByCity.csv", header=True, inferSchema=True)
                st.session_state.df_spark = df_spark
            except Exception as e:
                st.error(f"❌ Error cargando datos: {e}")
                return
    
    df_spark = st.session_state.df_spark

    # Aplicar normalización automáticamente
    if 'normalized' not in st.session_state:
        with st.spinner("Aplicando normalización..."):
            # try:
                normalizer = DataNormalizer(spark)
                df_spark = normalizer.normalize(df_spark)
                              
                st.session_state.df_spark = df_spark
                st.session_state.normalized = True
                st.success("✅ Normalización aplicada y datos actualizados en HDFS")
            # except Exception as e:
            #     st.error(f"❌ Error en normalización: {e}")
            #     return
    
    # # Ejecutar preEDA (usa la clase PreEDAAnalyzer)
    # analyzer = PreEDAAnalyzer(spark)
    # if 'preEDA_stats' not in st.session_state:
    #     with st.spinner("Ejecutando preEDA..."):
    #         # usar directamente el DataFrame df_spark en lugar de la ruta HDFS
    #         stats_df, plots = analyzer.analyze(df_spark)
    #         st.session_state.preEDA_stats = stats_df
    #         st.session_state.preEDA_plots = plots

    # stats_df = st.session_state.preEDA_stats

    # st.markdown("---")

    # # Mostrar estadísticas preEDA
    # st.subheader("📊 Estado del Dataset (preEDA)")
    # st.dataframe(stats_df, use_container_width=True)

    # # Mostrar plots (preEDA)
    # if 'preEDA_plots' in st.session_state and st.session_state.preEDA_plots:
    #     st.subheader("📈 Plots (preEDA)")
    #     plots = st.session_state.preEDA_plots

    #     # Normalizar a lista de (nombre, figura)
    #     items = []
    #     if isinstance(plots, dict):
    #         items = list(plots.items())
    #     elif isinstance(plots, (list, tuple)):
    #         if plots and isinstance(plots[0], tuple) and isinstance(plots[0][0], str):
    #             items = list(plots)
    #         else:
    #             items = [(f"plot_{i}", p) for i, p in enumerate(plots)]
    #     else:
    #         items = [("plot", plots)]

    #     for name, fig in items:
    #         try:
    #             # Matplotlib Figure
    #             try:
    #                 if isinstance(fig, Figure):
    #                     st.pyplot(fig)
    #                     continue
    #             except Exception:
    #                 pass

    #             # Plotly Figure
    #             try:
    #                 if isinstance(fig, go.Figure):
    #                     st.plotly_chart(fig, use_container_width=True)
    #                     continue
    #             except Exception:
    #                 pass

    #             # Image bytes / PIL / numpy array
    #             if isinstance(fig, (bytes, bytearray)):
    #                 st.image(fig, caption=name, use_column_width=True)
    #             else:
    #                 # Fallback: write object (alt text/representation)
    #                 st.write(f"**{name}**")
    #                 st.write(fig)
    #         except Exception as e:
    #             st.write(f"Error mostrando {name}: {e}")
    
    df_spark = st.session_state.df_spark
    # Botón para limpiar (clean e imputer)
    if st.button("🧹 Limpiar Dataset"):
        with st.spinner("Aplicando limpieza y imputación..."):
            try:
                df_spark = df_spark.repartition(8)
                df_spark = DataCleaner(spark).clean(df_spark)
                df_spark = KNNImputer(spark).impute(df_spark)
                st.session_state.df_spark = df_spark
                st.success("✅ Limpieza e imputación aplicadas")

                # Guardar el dataset limpio en HDFS; Spark write returns None, so catch exceptions instead of checking a return value
                try:
                    df_spark.write.mode("overwrite").parquet("hdfs://namenode:9000/clima/cleaned_dataset.parquet")
                    st.success("✅ Dataset guardado en HDFS: hdfs://namenode:9000/clima/cleaned_dataset.parquet")
                except Exception as e:
                    st.error(f"❌ Error guardando el dataset limpio en HDFS: {e}")
                    return
            except Exception as e:
                st.error(f"❌ Error durante limpieza/imputación: {e}")
                return