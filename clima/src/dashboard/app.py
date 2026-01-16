import streamlit as st
from streamlit_option_menu import option_menu
import sys
import os

# AÑADIDO: asegurar que el directorio 'src' del proyecto esté en sys.path
# app.py está en /app/src/dashboard -> subimos un nivel para apuntar a /app/src
SRC_DIR = os.path.abspath(os.path.dirname(__file__))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Importar páginas
from pages import (
    page_00_home,
    page_01_cargar_dataset,
    page_02_limpiar_dataset,
    page_03_eda,
    page_04_predicciones,
    page_05_visualizaciones
)

# Configuración de la página
st.set_page_config(
    page_title="Dashboard Clima",
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="expanded"
)


# Estilos CSS personalizados
st.markdown("""
    <style>
    [data-testid="stSidebar"] {
        width: 20% !important;
    }
    [data-testid="stMainBlockContainer"] {
        width: 80% !important;
    }
    </style>
""", unsafe_allow_html=True)

# --- NUEVO: helper para crear/obtener SparkSession centralizada ---
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

def create_or_get_spark_session():
    """
    Crea (o devuelve) una SparkSession centralizada para toda la app.
    Lee parámetros desde variables de entorno y aplica ajustes recomendados.
    Hace intento de usar YARN y cae a 'local[*]' si hay errores de conexión/contexto.
    """
    if 'spark' in st.session_state:
        return st.session_state.spark

    # Leer variables de entorno con valores por defecto
    driver_mem = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    executor_mem = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    executor_cores = int(os.getenv("SPARK_EXECUTOR_CORES", "2"))
    executor_instances = int(os.getenv("SPARK_EXECUTOR_INSTANCES", "1"))
    default_shuffle_partitions = str(max(8, executor_instances * executor_cores * 2))
    shuffle_partitions = int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", default_shuffle_partitions))

    # Intentar crear en modo YARN (client) — si falla, fallback a local[*]
    try:
        ss = (
            SparkSession.builder
            .appName("ClimaDashboard")
            .master("yarn")
            .config("spark.submit.deployMode", "client")
            .config("spark.driver.memory", driver_mem)
            .config("spark.executor.memory", executor_mem)
            .config("spark.executor.cores", executor_cores)
            .config("spark.executor.instances", executor_instances)
            .config("spark.sql.shuffle.partitions", shuffle_partitions)
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate()
        )
        mode = "yarn"
    except Py4JJavaError as e:
        err_text = str(e)
        print("SparkSession YARN error (full):", err_text)  # Log full error to console for debugging
        st.warning("⚠️ No se pudo crear SparkSession sobre YARN, usando modo local como fallback. Error: " + (err_text[:200] if err_text else "sin detalle"))
        st.warning("⚠️ No se pudo crear SparkSession sobre YARN, usando modo local como fallback. Error: " + (err_text[:200] if err_text else "sin detalle"))
        ss = (
            SparkSession.builder
            .appName("ClimaDashboard-local-fallback")
            .master("local[*]")
            .config("spark.driver.memory", driver_mem)
            .config("spark.executor.memory", executor_mem)
            .config("spark.sql.shuffle.partitions", shuffle_partitions)
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate()
        )
        mode = "local"

    # Ajustes runtime para mitigar OOM/GC pressure en ejecuciones costosas
    try:
        ss.conf.set("spark.memory.fraction", "0.75")
        ss.conf.set("spark.memory.storageFraction", "0.3")
        ss.conf.set("spark.reducer.maxSizeInFlight", "24m")
        ss.conf.set("spark.shuffle.spill.compress", "true")
        ss.conf.set("spark.shuffle.compress", "true")
        # fijar el número de particiones de shuffle si no se configuró
        ss.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    except Exception:
        pass

    # reducir verbosidad de logs
    try:
        ss.sparkContext.setLogLevel("WARN")
    except Exception:
        pass

    # Mostrar mensaje simple sin detalles internos
    st.info(f"SparkSession inicializada correctamente en modo: {mode}.")
    return ss
    return ss
# --- FIN helper ---

def main():
    # Crear/obtener SparkSession centralizada antes de renderizar páginas
    spark = create_or_get_spark_session()

    # Sidebar con opciones de navegación
    with st.sidebar:
        st.markdown("## 🌦️ CLIMA")
        st.markdown("---")
        
        selected = option_menu(
            menu_title="Módulos",
            options=[
                "Home",
                "Cargar Dataset",
                "Limpiar Dataset",
                "Análisis Exploratorio",
                "Predicciones",
                "Visualizaciones"
            ],
            icons=[
                "house",
                "cloud-upload",
                "brush",
                "bar-chart",
                "crystal-ball",
                "graph-up"
            ],
            menu_icon="cast",
            default_index=0,
            orientation="vertical"
        )
        
        st.markdown("---")
        st.markdown("### 📊 Información")
        st.info("Dashboard para análisis y predicción de datos meteorológicos")
    
    # Contenido principal basado en la selección
    # Llamar a la página pasando la SparkSession cuando sea posible.
    # Si la página aún no acepta el parámetro 'spark', hacemos fallback a la llamada sin argumentos.
    def _call_page(fn):
        try:
            fn(spark)
        except TypeError:
            fn()

    if selected == "Home":
        _call_page(page_00_home.show)
    elif selected == "Cargar Dataset":
        _call_page(page_01_cargar_dataset.show)
    elif selected == "Limpiar Dataset":
        _call_page(page_02_limpiar_dataset.show)
    elif selected == "Análisis Exploratorio":
        _call_page(page_03_eda.show)
    elif selected == "Predicciones":
        _call_page(page_04_predicciones.show)
    elif selected == "Visualizaciones":
        _call_page(page_05_visualizaciones.show)

if __name__ == "__main__":
    main()
