#!/bin/bash

echo "=========================================="
echo "  INICIANDO APLICACIÓN CLIMA"
echo "=========================================="

# Función para verificar si el namenode está disponible
wait_for_namenode() {
    echo "⏳ Esperando a que HDFS Namenode esté disponible..."
    
    max_attempts=30
    attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        echo "   Intento $attempt de $max_attempts..."
        
        # Intentar conectar al namenode usando nc (netcat) o curl
        if command -v nc &> /dev/null; then
            if nc -z namenode 9000 2>/dev/null; then
                echo "✅ Namenode está disponible en el puerto 9000"
                break
            fi
        else
            # Usar timeout con bash
            if (echo > /dev/tcp/namenode/9000) 2>/dev/null; then
                echo "✅ Namenode está disponible en el puerto 9000"
                break
            fi
        fi
        
        if [ $attempt -eq $max_attempts ]; then
            echo "⚠️  Tiempo de espera agotado. Continuando de todos modos..."
        fi
        
        sleep 5
        attempt=$((attempt + 1))
    done
    
    # Espera adicional para que los servicios estén completamente listos
    echo "⏳ Esperando 10 segundos adicionales para estabilización..."
    sleep 10
}

# Función para verificar si Spark Master está disponible
wait_for_spark() {
    echo "⏳ Esperando a que Spark Master esté disponible..."
    
    max_attempts=20
    attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        echo "   Intento $attempt de $max_attempts..."
        
        if (echo > /dev/tcp/spark-master/7077) 2>/dev/null; then
            echo "✅ Spark Master está disponible en el puerto 7077"
            break
        fi
        
        if [ $attempt -eq $max_attempts ]; then
            echo "⚠️  Tiempo de espera agotado. Continuando de todos modos..."
        fi
        
        sleep 5
        attempt=$((attempt + 1))
    done
}

# Esperar a los servicios si está habilitado
if [ "${WAIT_FOR_HDFS}" = "true" ]; then
    wait_for_namenode
    wait_for_spark
fi

echo ""
echo "=========================================="
echo "  INICIANDO STREAMLIT"
echo "=========================================="
echo ""

# Iniciar Streamlit
cd /app
exec streamlit run src/dashboard/app.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false
