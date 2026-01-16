# Dockerfile: base con Java 17 (Temurin) y Python
FROM eclipse-temurin:17-jdk-jammy

LABEL maintainer="clima" description="Python + Java17 + Hadoop client"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    STREAMLIT_SERVER_MAX_UPLOAD_SIZE=1024 \
    JAVA_HOME=/usr/lib/jvm/java-17-temurin

# Instalar Python 3.11 y utilidades
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 python3.11-venv python3-pip python3-distutils \
    ca-certificates curl netcat-openbsd wget procps gnupg \
    && rm -rf /var/lib/apt/lists/*

# Opcional: crear symlinks para python/pip si los necesitas
RUN ln -s /usr/bin/python3.11 /usr/bin/python && ln -s /usr/bin/pip3 /usr/bin/pip || true

# Instalar cliente Hadoop
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz \
    && tar -xzf hadoop-3.2.1.tar.gz -C /opt \
    && mv /opt/hadoop-3.2.1 /opt/hadoop \
    && rm hadoop-3.2.1.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin

# Crear usuario no-root y preparar app dir
RUN useradd -m -s /bin/bash appuser \
    && mkdir -p /app /app/logs /tmp/hadoop-root \
    && chown -R appuser:appuser /app /app/logs /tmp/hadoop-root /opt/hadoop

WORKDIR /app

# Copiar requirements e instalar dependencias Python
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# Copiar configs Hadoop (asegúrate de tenerlos en repo)
COPY core-site.xml $HADOOP_CONF_DIR/
COPY yarn-site.xml $HADOOP_CONF_DIR/
COPY hdfs-site.xml $HADOOP_CONF_DIR/

# Copiar app y dar permisos
COPY --chown=appuser:appuser . /app/
RUN chmod +x /app/start_app.sh && chown appuser:appuser /app/start_app.sh

EXPOSE 4040 8501

USER appuser
CMD ["/bin/bash", "/app/start_app.sh"]
