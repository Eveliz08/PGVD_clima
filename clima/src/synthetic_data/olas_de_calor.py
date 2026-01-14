"""
Criterios para determinar una ola de calor:
    Temperatura mayor que el percentil 90 de la serie histórica de temperatura en ese lugar en esa fecha
    Duración mínima de 3 días consecutivos.
    Tal vez se pueda tener en cuenta la tasa de calentamiento global de alguna forma

Crear dataset y almacenar en HDFS.
nombre del dataset: olas de calor, cada fila representa la ocurrencia de este fenómeno y recoge la información:
fecha de inicio del fenómeno
fecha de fin del fenómeno
País
Ciudad
Promedio de temperatura
Temperatura max alcanzada en estos días.
Humedad
Sequía térmica (Sí o No)
"""