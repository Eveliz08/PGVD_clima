#!/usr/bin/env python3
"""
Módulo para descargar el dataset GlobalLandTemperaturesByCity.csv de Kaggle
y subirlo a HDFS en la carpeta /clima.
"""

import os
import sys
import subprocess
import zipfile
import tempfile
from pathlib import Path

# Configuración del dataset
DATASET = "berkeleyearth/climate-change-earth-surface-temperature-data"
FILE_NAME = "GlobalLandTemperaturesByCity.csv"
HDFS_PATH = "/clima"
HDFS_NAMENODE_UI = "http://localhost:9870"  # URL de la interfaz web de HDFS

def run_command(cmd, description):
    """Ejecuta un comando y devuelve (success, stdout_or_error)."""
    print(f"⏳ {description}...")
    try:
        result = subprocess.run(cmd, shell=True, check=True,
                                capture_output=True, text=True)
        stdout = result.stdout.strip() if result.stdout else ""
        print(f"✅ {description} completado.")
        if stdout:
            print(f"   Salida: {stdout[:1000]}")
        return True, stdout
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.strip() if e.stderr else (e.stdout.strip() if e.stdout else 'Sin mensaje de error')
        print(f"❌ Error al {description.lower()}:")
        print(f"   Código de salida: {e.returncode}")
        print(f"   Error: {error_msg}")
        return False, error_msg

def check_file_exists_in_hdfs(hdfs_path=HDFS_PATH, file_name=FILE_NAME):
    """
    Verifica si el archivo ya existe en HDFS.
    
    Returns:
        tuple: (exists: bool, message: str)
    """
    full_path = f"{hdfs_path}/{file_name}"
    cmd = f"hdfs dfs -test -e {full_path}"
    try:
        subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        return True, f"El archivo ya existe en HDFS: {full_path}"
    except subprocess.CalledProcessError:
        return False, f"El archivo no existe en HDFS: {full_path}"

def check_kaggle_config():
    """Verifica que la CLI de Kaggle esté configurada.
    Comprueba en orden:
      - KAGGLE_CONFIG_DIR (contenga kaggle.json)
      - combinación KAGGLE_USERNAME + KAGGLE_API_TOKEN (o KAGGLE_KEY)
      - ~/.kaggle/kaggle.json
    """
    # 1) Revisar variable de entorno KAGGLE_CONFIG_DIR
    kaggle_env = os.getenv("KAGGLE_CONFIG_DIR")
    if kaggle_env:
        kaggle_path = Path(kaggle_env) / 'kaggle.json'
        if kaggle_path.exists():
            return True, f"Configuración de Kaggle encontrada en KAGGLE_CONFIG_DIR={kaggle_env}"
        else:
            return False, f"No se encontró kaggle.json en KAGGLE_CONFIG_DIR={kaggle_env}"

    # 2) Revisar variables de entorno KAGGLE_USERNAME + KAGGLE_API_TOKEN (aceptamos token como key)
    kaggle_user = os.getenv("KAGGLE_USERNAME")
    kaggle_api_token = os.getenv("KAGGLE_API_TOKEN")
    if kaggle_user and kaggle_api_token:
        return True, "Configuración de Kaggle encontrada en variables de entorno (KAGGLE_USERNAME + KAGGLE_API_TOKEN)"

    # 3) Revisar variable legacy KAGGLE_KEY
    kaggle_key = os.getenv("KAGGLE_KEY")
    if kaggle_user and kaggle_key:
        return True, "Configuración de Kaggle encontrada en variables de entorno (KAGGLE_USERNAME + KAGGLE_KEY)"

    # 4) Revisar ubicación por defecto en el HOME del proceso
    kaggle_path = Path.home() / '.kaggle' / 'kaggle.json'
    if kaggle_path.exists():
        return True, f"Configuración de Kaggle encontrada en {kaggle_path}"

    msg = ("No se encontró el archivo de configuración de Kaggle (~/.kaggle/kaggle.json) ni variables de entorno KAGGLE_USERNAME/KAGGLE_API_TOKEN. "
           "Para configurar Kaggle: 1) Ve a https://www.kaggle.com/account, 2) Clic en 'Create New API Token' para descargar kaggle.json, "
           "3) Colócalo en ~/.kaggle/kaggle.json o móntalo como volumen en el servicio 'clima', "
           "4) O exporta variables de entorno en docker-compose: KAGGLE_USERNAME y KAGGLE_API_TOKEN (o KAGGLE_KEY).")
    return False, msg

def check_dependencies():
    """Verifica que las herramientas necesarias estén instaladas."""
    print("🔍 Verificando dependencias...")
    for cmd, tool in [("kaggle --version", "Kaggle CLI"), 
                     ("hdfs dfs -help", "Hadoop HDFS")]:
        success, error = run_command(cmd, f"Verificando {tool}")
        if not success:
            return False, f"No se encontró {tool}. Asegúrate de que esté instalado y en PATH."
    return True, "Todas las dependencias están instaladas"

def download_kaggle_dataset(dataset, file_name, tmpdir):
    """Descarga un dataset de Kaggle."""
    download_cmd = f"kaggle datasets download -d {dataset} -f {file_name} -p {tmpdir}"
    return run_command(download_cmd, f"Descargando {file_name} de Kaggle")

def extract_zip(zip_path, tmpdir):
    """Extrae un archivo ZIP."""
    print(f"📦 Extrayendo archivo ZIP...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(tmpdir)
        print(f"✅ Archivo extraído.")
        return True, None
    except Exception as e:
        return False, f"Error al extraer el archivo ZIP: {e}"

def upload_to_hdfs(csv_path, hdfs_path, file_name, block_size_bytes=134217728):
    """Sube un archivo a HDFS usando un tamaño de bloque específico (por defecto 128 MiB)."""
    # Crear directorio en HDFS si no existe
    mkdir_cmd = f"hdfs dfs -mkdir -p {hdfs_path}"
    success, out = run_command(mkdir_cmd, f"Creando directorio HDFS {hdfs_path}")
    if not success:
        return False, f"No se pudo crear el directorio HDFS. Verifica que el servicio esté activo. Error: {out}"
    
    # Subir archivo a HDFS forzando blocksize
    # Usamos -Ddfs.blocksize=<bytes> para forzar block size en la operación de cliente
    upload_cmd = f"hdfs dfs -Ddfs.blocksize={block_size_bytes} -put -f {csv_path} {hdfs_path}/{file_name}"
    success, out = run_command(upload_cmd, f"Subiendo {file_name} a HDFS (blocksize={block_size_bytes} bytes)")
    if not success:
        return False, out
    return True, out

def verify_hdfs_upload(hdfs_path, file_name):
    """Verifica que el archivo se haya subido correctamente a HDFS y devuelve info de bloques."""
    full_path = f"{hdfs_path}/{file_name}"
    # Intentar obtener info detallada de bloques con fsck
    fsck_cmd = f"hdfs fsck {full_path} -files -blocks -locations"
    success, out = run_command(fsck_cmd, f"Ejecutando fsck para {full_path}")
    if success:
        return True, out
    # Si fsck falla, intentar con ls como fallback
    verify_cmd = f"hdfs dfs -ls {full_path}"
    success2, out2 = run_command(verify_cmd, "Verificando archivo en HDFS (ls fallback)")
    if success2:
        return True, out2
    return False, out or out2

def download_and_upload_to_hdfs(force=False):
    """
    Función principal que orquesta la descarga y subida a HDFS.
    
    Args:
        force: Si es True, descarga aunque el archivo ya exista en HDFS.
    
    Returns:
        dict: {
            'success': bool,
            'message': str,
            'hdfs_path': str (si tuvo éxito),
            'file_size_gb': float (si tuvo éxito)
        }
    """
    result = {
        'success': False,
        'message': '',
        'hdfs_path': None,
        'file_size_gb': None,
        'fsck': None
    }
    
    print("=" * 60)
    print("SCRIPT DE DESCARGA KAGGLE A HDFS")
    print("=" * 60)
    
    # Verificar si el archivo ya existe en HDFS
    if not force:
        exists, msg = check_file_exists_in_hdfs()
        if exists:
            result['success'] = True
            result['message'] = msg
            result['hdfs_path'] = f"{HDFS_PATH}/{FILE_NAME}"
            # intentar obtener fsck info para el archivo ya existente
            v_ok, v_out = verify_hdfs_upload(HDFS_PATH, FILE_NAME)
            result['fsck'] = v_out
            print(f"ℹ️  {msg}")
            return result

    # Verificar dependencias
    success, msg = check_dependencies()
    if not success:
        result['message'] = msg
        return result
    
    # Verificar configuración de Kaggle
    success, msg = check_kaggle_config()
    if not success:
        result['message'] = msg
        return result
    
    # Crear directorio temporal
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"\n📁 Directorio temporal creado: {tmpdir}")
        
        # 1. Descargar dataset de Kaggle
        success, error = download_kaggle_dataset(DATASET, FILE_NAME, tmpdir)
        if not success:
            result['message'] = f"Error al descargar de Kaggle: {error}"
            return result
        
        zip_path = os.path.join(tmpdir, f"{FILE_NAME}.zip")
        csv_path = os.path.join(tmpdir, FILE_NAME)
        
        # 2. Extraer el archivo CSV
        success, error = extract_zip(zip_path, tmpdir)
        if not success:
            result['message'] = error
            return result
        
        # 3. Subir a HDFS
        success, error = upload_to_hdfs(csv_path, HDFS_PATH, FILE_NAME)
        if not success:
            result['message'] = f"Error al subir a HDFS: {error}"
            return result
        
        # 4. Verificar la subida
        success, out = verify_hdfs_upload(HDFS_PATH, FILE_NAME)
        if success:
            file_size_gb = os.path.getsize(csv_path) / (1024**3)
            result['success'] = True
            result['message'] = f"Dataset descargado y subido exitosamente a HDFS"
            result['hdfs_path'] = f"{HDFS_PATH}/{FILE_NAME}"
            result['file_size_gb'] = file_size_gb
            result['fsck'] = out
            
            print("\n" + "=" * 60)
            print("🎉 PROCESO COMPLETADO EXITOSAMENTE")
            print("=" * 60)
            print(f"📊 Dataset: {FILE_NAME}")
            print(f"📁 Ubicación HDFS: {HDFS_PATH}/{FILE_NAME}")
            print(f"💾 Tamaño: {file_size_gb:.2f} GB")
        else:
            result['message'] = "No se pudo verificar la subida a HDFS."
            result['fsck'] = out

    return result

def get_hdfs_ui_url():
    """Retorna la URL de la interfaz web de HDFS."""
    return HDFS_NAMENODE_UI

if __name__ == "__main__":
    download_and_upload_to_hdfs()