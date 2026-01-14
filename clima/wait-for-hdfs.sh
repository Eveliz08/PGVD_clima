#!/bin/bash

# Función para esperar a que un puerto esté disponible
wait_for_port() {
    local host=$1
    local port=$2
    local max_attempts=30
    local attempt=1
    
    echo "Esperando $host:$port..."
    
    while ! nc -z $host $port; do
        if [ $attempt -ge $max_attempts ]; then
            echo "ERROR: $host:$port no disponible después de $max_attempts intentos"
            exit 1
        fi
        
        echo "Intento $attempt/$max_attempts: $host:$port no disponible, esperando..."
        sleep 10
        attempt=$((attempt+1))
    done
    
    echo "$host:$port está disponible"
}

# Esperar por namenode
wait_for_port namenode 9000
wait_for_port namenode 9870

# Esperar por datanode
wait_for_port datanode 9864

# Esperar que HDFS esté listo (usando curl para verificar estado)
echo "Verificando estado de HDFS..."
max_hdfs_attempts=30
hdfs_attempt=1

while true; do
    # Verificar usando la API REST del namenode
    if curl -s "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus" | grep -q "active"; then
        echo "HDFS está activo"
        break
    fi
    
    if [ $hdfs_attempt -ge $max_hdfs_attempts ]; then
        echo "ERROR: HDFS no se activó después de $max_hdfs_attempts intentos"
        exit 1
    fi
    
    echo "Intento $hdfs_attempt/$max_hdfs_attempts: HDFS no está listo, esperando..."
    sleep 10
    hdfs_attempt=$((hdfs_attempt+1))
done

# Crear directorio en HDFS si no existe (opcional)
echo "Creando directorios en HDFS si no existen..."
hadoop fs -mkdir -p /user/clima/data 2>/dev/null || true
hadoop fs -chmod 777 /user/clima 2>/dev/null || true

echo "HDFS está listo. Iniciando aplicación..."
exec python app.py