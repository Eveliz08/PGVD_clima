import streamlit as st

def show():
    """Página de inicio con información de bienvenida."""
    
    st.title("🌦️ Bienvenido al Dashboard de Análisis Climático")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ## 📋 Descripción del Proyecto
        
        Este dashboard proporciona herramientas completas para:
        
        - **Cargar y gestionar** datasets meteorológicos
        - **Limpiar y procesar** datos con técnicas avanzadas
        - **Explorar y analizar** patrones climáticos
        - **Generar predicciones** usando modelos de Machine Learning
        - **Visualizar** resultados de forma interactiva
        """)
    
    with col2:
        st.markdown("""
        ## 🚀 Módulos Disponibles
        
        1. **Cargar Dataset** - Importar archivos CSV o conectar a HDFS
        2. **Limpiar Dataset** - Imputación, normalización y transformaciones
        3. **Análisis Exploratorio** - Estadísticas y correlaciones
        4. **Predicciones** - Modelos ML para pronósticos
        5. **Visualizaciones** - Gráficos interactivos personalizados
        """)
    
    st.markdown("---")
    
    st.markdown("""
    ## 📊 Características Principales
    
    ### Preprocesamiento Avanzado
    - Imputación de valores faltantes con KNN
    - Normalización y estandarización de datos
    - Detección y manejo de outliers
    
    ### Análisis Exploratorio
    - Estadísticas descriptivas
    - Análisis de correlaciones
    - Distribuciones de variables
    
    ### Predicciones
    - Modelos de regresión y clasificación
    - Validación cruzada
    - Evaluación de métricas de desempeño
    
    ### Visualizaciones
    - Gráficos interactivos con Plotly
    - Mapas de calor
    - Series de tiempo
    """)
    
    st.markdown("---")
    
    st.info("""
    **💡 Consejo:** Comienza por cargar un dataset en el módulo "Cargar Dataset" 
    para explorar todos los análisis y predicciones disponibles.
    """)
    
    # Footer
    st.markdown("""
    <hr style="margin-top: 50px;">
    <p style="text-align: center; color: gray;">
        Dashboard de Análisis Climático | 2024
    </p>
    """, unsafe_allow_html=True)
