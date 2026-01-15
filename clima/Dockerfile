# Imagen base: Python 3.11 slim (Debian)
FROM python:3.11-slim

# Etiqueta del contenedor
LABEL maintainer="clima" \
      description="Contenedor con Python 3.11 y pipeline de procesamiento de datos climáticos"

# Variables de entorno
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    STREAMLIT_SERVER_MAX_UPLOAD_SIZE=1024

# 1. Instalar dependencias del sistema necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    default-jre-headless \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Variable de entorno para Java (necesario para PySpark)
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 2. Instalar cliente HDFS (Hadoop)
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz \
    && tar -xzf hadoop-3.2.1.tar.gz -C /opt \
    && mv /opt/hadoop-3.2.1 /opt/hadoop \
    && rm hadoop-3.2.1.tar.gz

# 3. Configurar variables de entorno para Hadoop
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV HADOOP_COMMON_HOME=$HADOOP_HOME
ENV HADOOP_HDFS_HOME=$HADOOP_HOME
ENV HADOOP_YARN_HOME=$HADOOP_HOME
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# 4. Configurar permisos para Hadoop
RUN chmod +x $HADOOP_HOME/bin/* \
    && chmod +x $HADOOP_HOME/sbin/*

# 5. Crear directorio de la aplicación
WORKDIR /app

# 6. Copiar requirements primero (para aprovechar cache de Docker)
COPY requirements.txt /app/

# 7. Instalar dependencias Python
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# 8. Copiar archivo de configuración de Hadoop
COPY core-site.xml $HADOOP_CONF_DIR/

# 9. Copiar el resto del proyecto
COPY . /app/

# 10. Crear directorios de logs
RUN mkdir -p /app/logs \
    && mkdir -p /tmp/hadoop-root

# 11. Configurar permisos para directorios temporales
RUN chmod -R 777 /tmp/hadoop-root \
    && chmod -R 777 /app/logs

# 12. Hacer ejecutable el script de arranque
RUN chmod +x /app/start_app.sh

# Exponer puertos (Spark UI y Streamlit)
EXPOSE 4040 8501

# Ejecutar script que espera servicios y lanza Streamlit
CMD ["/bin/bash", "/app/start_app.sh"]