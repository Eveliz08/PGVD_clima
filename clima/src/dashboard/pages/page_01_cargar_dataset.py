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
    check_kaggle_config,
    get_hdfs_ui_url,
    HDFS_PATH,
    FILE_NAME,
    upload_to_hdfs
)

def show():
    """Página para cargar y visualizar datasets."""
    
    st.title("📤 Cargar Dataset")
    
    # Link a la interfaz de HDFS
    hdfs_ui_url = get_hdfs_ui_url()
    st.info(f"🔗 Interfaz web de HDFS: {hdfs_ui_url}")
    
    st.markdown("---")
    
    loand = 0
    tab1, tab2 = st.tabs(["📥 Descargar de Kaggle", "📁 Cargar Archivo Local"])
    
    with tab1:
        st.subheader("Descargar Dataset de Kaggle a HDFS")
        st.markdown(
            "Este proceso descargará el dataset **GlobalLandTemperaturesByCity.csv** "
            "desde Kaggle y lo subirá automáticamente a HDFS."
        )
        
        # Verificar si el archivo ya existe en HDFS
        exists, msg = check_file_exists_in_hdfs()
        if exists:
            st.success(f"✅ {msg}")
            st.info(f"📁 Ruta HDFS: `{HDFS_PATH}/{FILE_NAME}`")
        
        
        st.markdown("---")
        col1, col2 = st.columns([1, 2])
        with col1:
            force_download = st.checkbox("Forzar descarga (sobrescribir si existe)")
        with col2:
            st.caption("Si el archivo ya está en HDFS, puedes forzar la descarga para sobrescribirlo.")
        
        if st.button("📥 Descargar Dataset de Kaggle"):

            with st.spinner("Descargando dataset de Kaggle y subiendo a HDFS... Esto puede tardar varios minutos."):
                resultado = download_and_upload_to_hdfs(force=force_download)
            if resultado.get('success'):
                st.success(f"✅ {resultado.get('message')}")
                st.info(f"📁 Ruta HDFS: `{resultado.get('hdfs_path')}`")
                if resultado.get('file_size_gb') is not None:
                    st.metric("Tamaño del archivo", f"{resultado['file_size_gb']:.2f} GB")
                loand = 1
                st.balloons()
            else:
                st.error(f"❌ Error: {resultado.get('message')}")
                if resultado.get('fsck'):
                    with st.expander("Salida de verificación (fsck / fallback)"):
                        st.code(resultado.get('fsck'))
        
    
    with tab2:
        st.subheader("Cargar Archivo Local a HDFS")
        st.markdown(
            "Selecciona un archivo CSV de tu computadora y cárgalo a HDFS."
        )
        
        uploaded_file = st.file_uploader(
            "Selecciona un archivo CSV",
            type=["csv"],
            help="Formato soportado: CSV",
            key="upload_csv_to_hdfs"
        )
        
        if uploaded_file is not None:
            col1, col2 = st.columns([1, 2])
            with col1:
                custom_filename = st.text_input(
                    "Nombre del archivo en HDFS",
                    value=uploaded_file.name,
                    help="Nombre con el que se guardará en HDFS"
                )
            with col2:

                # Guardar temporalmente y subir
                temp_path = f"/tmp/{uploaded_file.name}"
                with open(temp_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                    
                success, out = upload_to_hdfs(temp_path, HDFS_PATH, uploaded_file.name)
                
                if success:
                    st.success(f"✅ Archivo subido a HDFS en {HDFS_PATH}/{custom_filename}")
                    loand = 1
                    if out:
                        with st.expander("Detalles HDFS (put output)"):
                            st.code(out)
                else:
                    st.error(f"❌ Error al subir a HDFS: {out}")


    if loand:
        df = pd.read_csv(uploaded_file)
        st.session_state.df = df
            
            
        st.subheader("Vista previa del dataset")
        
        # Mostrar información del dataset si está cargado en sesión
        if "df" in st.session_state:
            st.markdown("---")
            st.subheader("📋 Información del Dataset Cargado")
                
            col1, col2, col3 = st.columns(3)
                
            with col1:
                st.metric("Filas", st.session_state.df.shape[0])
            with col2:
                st.metric("Columnas", st.session_state.df.shape[1])
            with col3:
                st.metric("Tipos de datos", len(st.session_state.df.dtypes.unique()))
                
            st.subheader("Vista previa del dataset")
            st.dataframe(st.session_state.df.head(50), use_container_width=True)
        loand = 0

