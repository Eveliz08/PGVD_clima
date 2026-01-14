"""
Se descarga desde Kaggle el dataset de temperaturas globales dentro del contenedor y se sube a HDFS.
"""

import streamlit as st
import pandas as pd
import os
import sys

# Añadir el path de tools al sistema
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'tools'))

from download_data import (
    download_and_upload_to_hdfs, 
    check_file_exists_in_hdfs, 
    get_hdfs_ui_url,
    HDFS_PATH,
    FILE_NAME
)

def show():
    """Página para cargar y visualizar datasets."""
    
    st.title("📤 Cargar Dataset")
    
    # Link a la interfaz de HDFS
    hdfs_ui_url = get_hdfs_ui_url()
    st.info(f"🔗 **Interfaz web de HDFS:** [{hdfs_ui_url}]({hdfs_ui_url}/explorer.html#/)")
    
    st.markdown("---")
    
    tab1, tab2 = st.tabs(["📥 Descargar de Kaggle", "📁 Cargar Archivo Local"])
    
    with tab1:
        st.subheader("Descargar Dataset de Kaggle a HDFS")
        st.markdown("""
        Este proceso descargará el dataset **GlobalLandTemperaturesByCity.csv** 
        desde Kaggle y lo subirá automáticamente a HDFS.
        """)
        
        # Verificar si el archivo ya existe en HDFS
        exists, msg = check_file_exists_in_hdfs()
        
        if exists:
            st.success(f"✅ {msg}")
            st.info(f"📁 Ruta HDFS: `{HDFS_PATH}/{FILE_NAME}`")
            
            # Opción para forzar la descarga
            force_download = st.checkbox("Forzar nueva descarga (sobrescribir archivo existente)")
            
            if st.button("🔄 Volver a descargar", disabled=not force_download):
                with st.spinner("Descargando y subiendo a HDFS..."):
                    resultado = download_and_upload_to_hdfs(force=True)
                    
                if resultado['success']:
                    st.success(f"✅ {resultado['message']}")
                    st.info(f"📁 Ruta HDFS: `{resultado['hdfs_path']}`")
                    if resultado['file_size_gb']:
                        st.metric("Tamaño del archivo", f"{resultado['file_size_gb']:.2f} GB")
                else:
                    st.error(f"❌ Error: {resultado['message']}")
        else:
            st.warning(f"⚠️ {msg}")
            
            if st.button("📥 Descargar Dataset de Kaggle"):
                with st.spinner("Descargando dataset de Kaggle y subiendo a HDFS... Esto puede tardar varios minutos."):
                    resultado = download_and_upload_to_hdfs()
                
                if resultado['success']:
                    st.success(f"✅ {resultado['message']}")
                    st.info(f"📁 Ruta HDFS: `{resultado['hdfs_path']}`")
                    if resultado['file_size_gb']:
                        st.metric("Tamaño del archivo", f"{resultado['file_size_gb']:.2f} GB")
                    st.balloons()
                else:
                    st.error(f"❌ Error: {resultado['message']}")
        
        st.markdown("---")
        st.markdown("### 📋 Requisitos")
        st.markdown("""
        - **Kaggle CLI** configurada con credenciales válidas
        - **HDFS** activo y accesible
        - Conexión a internet para descargar el dataset
        """)
    
    with tab2:
        st.subheader("Cargar desde archivo local")
        uploaded_file = st.file_uploader(
            "Selecciona un archivo CSV",
            type=["csv"],
            help="Formato soportado: CSV"
        )
        
        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file)
            st.session_state.df = df
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Filas", df.shape[0])
            with col2:
                st.metric("Columnas", df.shape[1])
            with col3:
                st.metric("Tipos de datos", len(df.dtypes.unique()))
            
            st.success("✅ Archivo cargado correctamente")
            
            st.subheader("Vista previa del dataset")
            st.dataframe(df.head(10), use_container_width=True)
    
    # Mostrar información del dataset si está cargado en sesión
    if "df" in st.session_state:
        st.markdown("---")
        st.subheader("📋 Información del Dataset Cargado")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Tipos de datos**")
            st.dataframe(
                st.session_state.df.dtypes.astype(str),
                use_container_width=True
            )
        
        with col2:
            st.markdown("**Valores faltantes**")
            missing = st.session_state.df.isnull().sum()
            st.dataframe(
                missing[missing > 0] if missing.sum() > 0 else pd.Series({"Sin valores faltantes": 0}),
                use_container_width=True
            )
