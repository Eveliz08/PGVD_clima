import streamlit as st
from streamlit_option_menu import option_menu
import sys
import os

# Configuración de la página
st.set_page_config(
    page_title="Dashboard Clima",
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Importar páginas
from pages import (
    page_00_home,
    page_01_cargar_dataset,
    page_02_limpiar_dataset,
    page_03_eda,
    page_04_predicciones,
    page_05_visualizaciones
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

def main():
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
    if selected == "Home":
        page_00_home.show()
    elif selected == "Cargar Dataset":
        page_01_cargar_dataset.show()
    elif selected == "Limpiar Dataset":
        page_02_limpiar_dataset.show()
    elif selected == "Análisis Exploratorio":
        page_03_eda.show()
    elif selected == "Predicciones":
        page_04_predicciones.show()
    elif selected == "Visualizaciones":
        page_05_visualizaciones.show()

if __name__ == "__main__":
    main()
