"""
Docstring for clima.src.synthetic_data.frío_extremo
Criterios para determinar una ola de frío
Temperatura por debajo del percentil 10
Caída abrupta de temperatura, más de 5 grados en 24 horas

Crear dataset y almacenar en HDFS.
nombre del dataset: olas de frío, cada fila representa la ocurrencia de este fenómeno y recoge la información:
fecha de inicio del fenómeno
fecha de fin del fenómeno
País
Ciudad
Promedio de temperatura
Temperatura min alcanzada en estos días.

"""