import streamlit as st
import pandas as pd

def show():
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
                st.session_state.df = df_spark.limit(1000).toPandas()  # Sample para display
                st.success("✅ Datos cargados desde HDFS")
            except Exception as e:
                st.error(f"❌ Error cargando datos: {e}")
                return
    
    df_spark = st.session_state.df_spark
    
    # Aplicar normalización automáticamente
    if 'normalized' not in st.session_state:
        with st.spinner("Aplicando normalización..."):
            try:
                df_spark = normalize(df_spark)
                st.session_state.df_spark = df_spark
                st.session_state.df = df_spark.limit(1000).toPandas()
                st.session_state.normalized = True
            except Exception as e:
                st.error(f"❌ Error en normalización: {e}")
                return
    
    # Ejecutar preEDA (usa la clase PreEDAAnalyzer)
    analyzer = PreEDAAnalyzer(spark)
    if 'preEDA_stats' not in st.session_state:
        with st.spinner("Ejecutando preEDA..."):
            try:
                stats_df, plots = analyzer.analyze("hdfs://namenode:9000/clima/GlobalLandTemperaturesByCity.csv")
                st.session_state.preEDA_stats = stats_df
                st.session_state.preEDA_plots = plots
            except Exception as e:
                st.error(f"❌ Error en preEDA: {e}")
                return
    
    stats_df = st.session_state.preEDA_stats
    
    st.markdown("---")
    
    # Mostrar estadísticas preEDA
    st.subheader("📊 Estado del Dataset (preEDA)")
    st.dataframe(stats_df, use_container_width=True)
    
    # Botón para limpiar (clean e imputer)
    if st.button("🧹 Limpiar Dataset"):
        with st.spinner("Aplicando limpieza y imputación..."):
            try:
                df_spark = clean(df_spark)
                df_spark = imputer(df_spark)
                st.session_state.df_spark = df_spark
                st.session_state.df = df_spark.limit(1000).toPandas()
                st.success("✅ Limpieza y imputación aplicadas")
                # Recalcular preEDA usando el analizador sobre la ruta en HDFS
                stats_df, plots = analyzer.analyze("hdfs://namenode:9000/clima/GlobalLandTemperaturesByCity.csv")
                st.session_state.preEDA_stats = stats_df
                st.session_state.preEDA_plots = plots
            except Exception as e:
                st.error(f"❌ Error en limpieza: {e}")
    
    st.markdown("---")
    
    # tab1, tab2, tab3, tab4 = st.tabs(use_container_width
    #     ["Valores Faltantes", "Outliers", "Normalización", "Transformaciones"]
    # )
    
    # with tab1:
    #     st.subheader("Manejo de Valores Faltantes")
        
    #     col1, col2 = st.columns(2)
        
    #     with col1:
    #         st.markdown("**Resumen de valores faltantes**")
    #         missing = st.session_state.df.isnull().sum()
    #         st.dataframe(missing[missing > 0], use_container_width=True)
        
    #     with col2:
    #         method = st.selectbox(
    #             "Selecciona método de imputación",
    #             ["Media", "Mediana", "Moda", "KNN"]
    #         )
            
    #         if st.button("Aplicar imputación"):
    #             st.success(f"✅ Imputación aplicada usando {method}")
    
    # with tab2:
    #     st.subheader("Detección y Manejo de Outliers")
        
    #     column = st.selectbox("Selecciona columna", st.session_state.df.columns)
    #     method = st.selectbox(
    #         "Método de detección",
    #         ["IQR", "Z-Score", "Isolation Forest"]
    #     )
        
    #     if st.button("Detectar outliers"):
    #         st.info(f"Detectando outliers en {column} usando {method}...")
    
    # with tab3:
    #     st.subheader("Normalización de Datos")
        
    #     norm_method = st.selectbox(
    #         "Selecciona método de normalización",
    #         ["Min-Max (0-1)", "Z-Score", "Robust Scaler"]
    #     )
        
    #     columns = st.multiselect(
    #         "Columnas a normalizar",
    #         st.session_state.df.select_dtypes(include=['float64', 'int64']).columns
    #     )
        
    #     if st.button("Aplicar normalización"):
    #         st.success(f"✅ Normalización {norm_method} aplicada")
    
    # with tab4:
    #     st.subheader("Transformaciones Adicionales")
        
    #     col1, col2 = st.columns(2)
        
    #     with col1:
    #         if st.checkbox("Eliminar duplicados"):
    #             st.success("✅ Duplicados eliminados")
            
    #         if st.checkbox("Convertir tipos de datos"):
    #             st.info("Configurar conversiones...")
        
    #     with col2:
    #         if st.checkbox("Crear variables derivadas"):
    #             st.info("Crear nuevas variables...")
            
    #         if st.checkbox("Filtrar datos"):
    #             st.info("Establecer criterios de filtrado...")
    
    # st.markdown("---")
    # st.markdown("**Resumen del dataset procesado**")
    # st.dataframe(st.session_state.df.head(), use_container_width=True)
