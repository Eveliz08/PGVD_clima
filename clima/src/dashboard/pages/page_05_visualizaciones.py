import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def show():
    """Página de visualizaciones interactivas."""
    
    st.title("📈 Visualizaciones")
    
    if "df" not in st.session_state:
        st.warning("⚠️ Por favor, carga un dataset primero en el módulo 'Cargar Dataset'")
        return
    
    df = st.session_state.df
    st.markdown("---")
    
    tab1, tab2, tab3, tab4 = st.tabs(
        ["Gráficos Básicos", "Series Temporales", "Mapas de Calor", "Personalizados"]
    )
    
    with tab1:
        st.subheader("Gráficos Básicos")
        
        col1, col2 = st.columns(2)
        
        with col1:
            graph_type = st.selectbox(
                "Tipo de gráfico",
                ["Histograma", "Dispersión", "Caja", "Barra"]
            )
        
        with col2:
            column = st.selectbox("Columna a visualizar", df.columns)
        
        if st.button("Generar gráfico"):
            st.info(f"Generando {graph_type} para {column}...")
    
    with tab2:
        st.subheader("Series Temporales")
        
        time_col = st.selectbox("Selecciona columna de tiempo", df.columns)
        value_col = st.selectbox("Selecciona columna de valores", df.columns)
        
        if st.button("Graficar serie temporal"):
            st.info(f"Visualizando serie temporal: {value_col} vs {time_col}...")
    
    with tab3:
        st.subheader("Mapas de Calor")
        
        if st.button("Generar mapa de calor de correlaciones"):
            st.info("Generando mapa de calor...")
    
    with tab4:
        st.subheader("Gráficos Personalizados")
        
        col1, col2 = st.columns(2)
        
        with col1:
            x_var = st.selectbox("Variable X", df.columns, key="viz_x")
        
        with col2:
            y_var = st.selectbox("Variable Y", df.columns, key="viz_y")
        
        color_var = st.selectbox(
            "Color por (opcional)",
            ["Ninguno"] + df.columns.tolist()
        )
        
        if st.button("Crear visualización personalizada"):
            st.info(f"Creando gráfico: {x_var} vs {y_var}...")
