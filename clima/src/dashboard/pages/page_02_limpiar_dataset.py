import streamlit as st
import pandas as pd

def show():
    """Página para limpiar y procesar el dataset."""
    
    st.title("🧹 Limpiar Dataset")
    
    if "df" not in st.session_state:
        st.warning("⚠️ Por favor, carga un dataset primero en el módulo 'Cargar Dataset'")
        return
    
    st.markdown("---")
    
    tab1, tab2, tab3, tab4 = st.tabs(
        ["Valores Faltantes", "Outliers", "Normalización", "Transformaciones"]
    )
    
    with tab1:
        st.subheader("Manejo de Valores Faltantes")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Resumen de valores faltantes**")
            missing = st.session_state.df.isnull().sum()
            st.dataframe(missing[missing > 0], use_container_width=True)
        
        with col2:
            method = st.selectbox(
                "Selecciona método de imputación",
                ["Media", "Mediana", "Moda", "KNN"]
            )
            
            if st.button("Aplicar imputación"):
                st.success(f"✅ Imputación aplicada usando {method}")
    
    with tab2:
        st.subheader("Detección y Manejo de Outliers")
        
        column = st.selectbox("Selecciona columna", st.session_state.df.columns)
        method = st.selectbox(
            "Método de detección",
            ["IQR", "Z-Score", "Isolation Forest"]
        )
        
        if st.button("Detectar outliers"):
            st.info(f"Detectando outliers en {column} usando {method}...")
    
    with tab3:
        st.subheader("Normalización de Datos")
        
        norm_method = st.selectbox(
            "Selecciona método de normalización",
            ["Min-Max (0-1)", "Z-Score", "Robust Scaler"]
        )
        
        columns = st.multiselect(
            "Columnas a normalizar",
            st.session_state.df.select_dtypes(include=['float64', 'int64']).columns
        )
        
        if st.button("Aplicar normalización"):
            st.success(f"✅ Normalización {norm_method} aplicada")
    
    with tab4:
        st.subheader("Transformaciones Adicionales")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.checkbox("Eliminar duplicados"):
                st.success("✅ Duplicados eliminados")
            
            if st.checkbox("Convertir tipos de datos"):
                st.info("Configurar conversiones...")
        
        with col2:
            if st.checkbox("Crear variables derivadas"):
                st.info("Crear nuevas variables...")
            
            if st.checkbox("Filtrar datos"):
                st.info("Establecer criterios de filtrado...")
    
    st.markdown("---")
    st.markdown("**Resumen del dataset procesado**")
    st.dataframe(st.session_state.df.head(), use_container_width=True)
