import streamlit as st
import pandas as pd
import numpy as np

def show():
    """Página de Análisis Exploratorio de Datos (EDA)."""
    
    st.title("📊 Análisis Exploratorio de Datos")
    
    if "df" not in st.session_state:
        st.warning("⚠️ Por favor, carga un dataset primero en el módulo 'Cargar Dataset'")
        return
    
    df = st.session_state.df
    st.markdown("---")
    
    tab1, tab2, tab3, tab4 = st.tabs(
        ["Estadísticas", "Correlaciones", "Distribuciones", "Relaciones"]
    )
    
    with tab1:
        st.subheader("📈 Estadísticas Descriptivas")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Resumen numérico**")
            st.dataframe(df.describe(), use_container_width=True)
        
        with col2:
            st.markdown("**Información del dataset**")
            info_data = {
                "Total de filas": df.shape[0],
                "Total de columnas": df.shape[1],
                "Memoria (MB)": df.memory_usage(deep=True).sum() / 1024**2,
                "Duplicados": df.duplicated().sum()
            }
            st.dataframe(pd.DataFrame(info_data.items(), columns=["Métrica", "Valor"]))
    
    with tab2:
        st.subheader("🔗 Matriz de Correlaciones")
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        if numeric_cols:
            st.info(f"Columnas numéricas detectadas: {len(numeric_cols)}")
            
            if st.checkbox("Mostrar matriz de correlaciones"):
                corr_matrix = df[numeric_cols].corr()
                st.dataframe(corr_matrix, use_container_width=True)
        else:
            st.warning("No hay columnas numéricas para calcular correlaciones")
    
    with tab3:
        st.subheader("📉 Distribuciones de Variables")
        
        column = st.selectbox("Selecciona una columna", df.columns)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"**Tipo de dato:** {df[column].dtype}")
            st.markdown(f"**Valores únicos:** {df[column].nunique()}")
            st.markdown(f"**Valores faltantes:** {df[column].isnull().sum()}")
        
        with col2:
            if df[column].dtype in ['float64', 'int64']:
                st.markdown("**Estadísticas**")
                st.dataframe(df[column].describe())
    
    with tab4:
        st.subheader("🔍 Análisis de Relaciones")
        
        col1, col2 = st.columns(2)
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        with col1:
            x_col = st.selectbox("Variable X", numeric_cols, key="x_var")
        
        with col2:
            y_col = st.selectbox("Variable Y", numeric_cols, key="y_var")
        
        if st.button("Analizar relación"):
            st.info(f"Analizando relación entre {x_col} y {y_col}...")
