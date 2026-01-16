#!/bin/bash

echo "=========================================="
echo "  INICIANDO APLICACIÓN CLIMA"
echo "=========================================="

# --- NUEVO: detectar java en tiempo de ejecución y fijar JAVA_HOME dinámicamente ---
if command -v java >/dev/null 2>&1; then
    JAVA_BIN=$(readlink -f "$(command -v java)")
    JAVA_HOME_DIR=$(dirname "$(dirname "$JAVA_BIN")")
    export JAVA_HOME="${JAVA_HOME_DIR}"
    export PATH="${JAVA_HOME}/bin:${PATH}"
    echo "🔧 JAVA detectado: ${JAVA_BIN}"
    echo "🔧 JAVA_HOME establecido en: ${JAVA_HOME}"
    java -version 2>&1 | sed -n '1,5p'
else
    echo "❌ java no está en PATH. Asegúrate de que la JRE/JDK esté instalada en la imagen."
    echo "    - Si usas una imagen base con Java (ej. eclipse-temurin), verifica que 'java' exista."
    echo "    - Alternativa: instala openjdk o temurin en el Dockerfile."
    exit 1
fi
# --- FIN NUEVO BLOQUE ---

# Establecer valores por defecto de memoria para Spark si no están definidos
: "${SPARK_DRIVER_MEMORY:=4g}"
: "${SPARK_EXECUTOR_MEMORY:=2g}"
: "${SPARK_WORKER_MEMORY:=6g}"
export SPARK_DRIVER_MEMORY SPARK_EXECUTOR_MEMORY SPARK_WORKER_MEMORY
echo "🔧 SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY}, SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY}, SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY}"

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

# Crear kaggle.json desde variables de entorno si están presentes
create_kaggle_config() {
    # Si ya existe KAGGLE_CONFIG_DIR con kaggle.json, no hacer nada
    if [ -n "${KAGGLE_CONFIG_DIR}" ] && [ -f "${KAGGLE_CONFIG_DIR}/kaggle.json" ]; then
        echo "ℹ️  kaggle.json ya existe en KAGGLE_CONFIG_DIR=${KAGGLE_CONFIG_DIR}"
        return
    fi

    # Usar HOME por defecto si no hay KAGGLE_CONFIG_DIR
    KAGGLE_DIR="${KAGGLE_CONFIG_DIR:-$HOME/.kaggle}"
    mkdir -p "${KAGGLE_DIR}"
    KAGGLE_FILE="${KAGGLE_DIR}/kaggle.json"

    # Si ya existe el archivo, informar y salir
    if [ -f "${KAGGLE_FILE}" ]; then
        echo "ℹ️  kaggle.json ya existe en ${KAGGLE_FILE}"
        export KAGGLE_CONFIG_DIR="${KAGGLE_DIR}"
        return
    fi

    # 1) Si el archivo kaggle.json fue montado por volumen (por ejemplo ./kaggle.json:/root/.kaggle/kaggle.json),
    #    el archivo ya estará presente y el bloque anterior lo detectará.

    # 2) Crear desde variables de entorno:
    #    - preferencia: KAGGLE_USERNAME + KAGGLE_API_TOKEN (token generado por Kaggle)
    if [ -n "${KAGGLE_USERNAME}" ] && [ -n "${KAGGLE_API_TOKEN}" ]; then
        echo "🔐 Creando kaggle.json desde KAGGLE_USERNAME + KAGGLE_API_TOKEN..."
        cat > "${KAGGLE_FILE}" <<EOF
{"username":"${KAGGLE_USERNAME}","key":"${KAGGLE_API_TOKEN}"}
EOF
        chmod 600 "${KAGGLE_FILE}"
        export KAGGLE_CONFIG_DIR="${KAGGLE_DIR}"
        echo "✅ kaggle.json creado en ${KAGGLE_FILE} y KAGGLE_CONFIG_DIR exportado"
        return
    fi

    # 3) Crear desde KAGGLE_USERNAME + KAGGLE_KEY (legacy)
    if [ -n "${KAGGLE_USERNAME}" ] && [ -n "${KAGGLE_KEY}" ]; then
        echo "🔐 Creando kaggle.json desde KAGGLE_USERNAME + KAGGLE_KEY..."
        cat > "${KAGGLE_FILE}" <<EOF
{"username":"${KAGGLE_USERNAME}","key":"${KAGGLE_KEY}"}
EOF
        chmod 600 "${KAGGLE_FILE}"
        export KAGGLE_CONFIG_DIR="${KAGGLE_DIR}"
        echo "✅ kaggle.json creado en ${KAGGLE_FILE} y KAGGLE_CONFIG_DIR exportado"
        return
    fi

    echo "ℹ️  No se proporcionaron credenciales de Kaggle en variables de entorno ni se montó kaggle.json."
}

# Llamar a la función para crear/configurar kaggle.json si procede
create_kaggle_config

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

# Ajustar límite de subida de Streamlit a 1GB (1024 MB)
export STREAMLIT_SERVER_MAX_UPLOAD_SIZE=1024

# Iniciar Streamlit
cd /app
exec streamlit run src/dashboard/app.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false \
    --server.maxUploadSize=1024
