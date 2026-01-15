[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/big-data-europe/Lobby)

# Entorno multi-contenedor Docker con Hadoop, Spark y Hive

Aquí está: un entorno multi-contenedor Docker con Hadoop (HDFS), Spark y Hive. Pero sin los grandes requisitos de memoria de un sandbox de Cloudera. (En mi portátil Windows 10 con WSL2 parece consumir solo 3 GB.)

Lo único que falta es que el servidor Hive no se inicia automáticamente. Se agregará cuando entienda cómo hacerlo en docker-compose.


## Inicio Rápido

Para desplegar el cluster HDFS-Spark-Hive, ejecuta:
```
  docker-compose up
```

`docker-compose` crea una red docker que se puede encontrar ejecutando `docker network list`, por ejemplo `docker-hadoop-spark-hive_default`.

Ejecuta `docker network inspect` en la red (por ejemplo `docker-hadoop-spark-hive_default`) para encontrar la IP en la que se publican las interfaces de hadoop. Accede a estas interfaces con las siguientes URLs:

* Namenode: http://<dockerhadoop_IP_address>:9870/dfshealth.html#tab-overview
* Servidor de historial: http://<dockerhadoop_IP_address>:8188/applicationhistory
* Datanode: http://<dockerhadoop_IP_address>:9864/
* Gestor de nodos: http://<dockerhadoop_IP_address>:8042/node
* Gestor de recursos: http://<dockerhadoop_IP_address>:8088/
* Maestro Spark: http://<dockerhadoop_IP_address>:8080/
* Worker Spark: http://<dockerhadoop_IP_address>:8081/
* Hive: http://<dockerhadoop_IP_address>:10000

## Nota importante sobre Docker Desktop
Desde que Docker Desktop desactivó "Exponer demonio en tcp://localhost:2375 sin TLS" por defecto, ha habido todo tipo de problemas de conexión ejecutando docker-compose completo. Activar esta opción nuevamente (Configuración > General > Exponer demonio en tcp://localhost:2375 sin TLS) hace que todo funcione. Todavía estoy buscando una solución más segura para esto.


## Inicio Rápido HDFS

Copia breweries.csv al namenode.
```
  docker cp breweries.csv namenode:breweries.csv
```

Ve al shell bash en el namenode con el mismo ID de contenedor del namenode.
```
  docker exec -it namenode bash
```


Crea un directorio HDFS /data/openbeer/breweries.

```
  hdfs dfs -mkdir -p /data/openbeer/breweries
```

Copia breweries.csv a HDFS:
```
  hdfs dfs -put breweries.csv /data/openbeer/breweries/breweries.csv
```


## Inicio Rápido Spark (PySpark)

Ve a http://<dockerhadoop_IP_address>:8080 o http://localhost:8080/ en tu host Docker (portátil) para ver el estado del maestro Spark.

Ve a la línea de comandos del maestro Spark e inicia PySpark.
```
  docker exec -it spark-master bash

  /spark/bin/pyspark --master spark://spark-master:7077
```

Carga breweries.csv desde HDFS.
```
  brewfile = spark.read.csv("hdfs://namenode:9000/data/openbeer/breweries/breweries.csv")
  
  brewfile.show()
+----+--------------------+-------------+-----+---+
| _c0|                 _c1|          _c2|  _c3|_c4|
+----+--------------------+-------------+-----+---+
|null|                name|         city|state| id|
|   0|  NorthGate Brewing |  Minneapolis|   MN|  0|
|   1|Against the Grain...|   Louisville|   KY|  1|
|   2|Jack's Abby Craft...|   Framingham|   MA|  2|
|   3|Mike Hess Brewing...|    San Diego|   CA|  3|
|   4|Fort Point Beer C...|San Francisco|   CA|  4|
|   5|COAST Brewing Com...|   Charleston|   SC|  5|
|   6|Great Divide Brew...|       Denver|   CO|  6|
|   7|    Tapistry Brewing|     Bridgman|   MI|  7|
|   8|    Big Lake Brewing|      Holland|   MI|  8|
|   9|The Mitten Brewin...| Grand Rapids|   MI|  9|
|  10|      Brewery Vivant| Grand Rapids|   MI| 10|
|  11|    Petoskey Brewing|     Petoskey|   MI| 11|
|  12|  Blackrocks Brewery|    Marquette|   MI| 12|
|  13|Perrin Brewing Co...|Comstock Park|   MI| 13|
|  14|Witch's Hat Brewi...|   South Lyon|   MI| 14|
|  15|Founders Brewing ...| Grand Rapids|   MI| 15|
|  16|   Flat 12 Bierwerks| Indianapolis|   IN| 16 |
|  17|Tin Man Brewing C...|   Evansville|   IN| 17|
|  18|Black Acre Brewin...| Indianapolis|   IN| 18|
+----+--------------------+-------------+-----+---+
only showing top 20 rows

```



## Inicio Rápido Spark (Scala)

Ve a http://<dockerhadoop_IP_address>:8080 o http://localhost:8080/ en tu host Docker (portátil) para ver el estado del maestro Spark.

Ve a la línea de comandos del maestro Spark e inicia spark-shell.
```
  docker exec -it spark-master bash
  
  spark/bin/spark-shell --master spark://spark-master:7077
```

Carga breweries.csv desde HDFS.
```
  val df = spark.read.csv("hdfs://namenode:9000/data/openbeer/breweries/breweries.csv")
  
  df.show()
+----+--------------------+-------------+-----+---+
| _c0|                 _c1|          _c2|  _c3|_c4|
+----+--------------------+-------------+-----+---+
|null|                name|         city|state| id|
|   0|  NorthGate Brewing |  Minneapolis|   MN|  0|
|   1|Against the Grain...|   Louisville|   KY|  1|
|   2|Jack's Abby Craft...|   Framingham|   MA|  2|
|   3|Mike Hess Brewing...|    San Diego|   CA|  3|
|   4|Fort Point Beer C...|San Francisco|   CA|  4|
|   5|COAST Brewing Com...|   Charleston|   SC|  5|
|   6|Great Divide Brew...|       Denver|   CO|  6|
|   7|    Tapistry Brewing|     Bridgman|   MI|  7|
|   8|    Big Lake Brewing|      Holland|   MI|  8|
|   9|The Mitten Brewin...| Grand Rapids|   MI|  9|
|  10|      Brewery Vivant| Grand Rapids|   MI| 10|
|  11|    Petoskey Brewing|     Petoskey|   MI| 11|
|  12|  Blackrocks Brewery|    Marquette|   MI| 12|
|  13|Perrin Brewing Co...|Comstock Park|   MI| 13|
|  14|Witch's Hat Brewi...|   South Lyon|   MI| 14|
|  15|Founders Brewing ...| Grand Rapids|   MI| 15|
|  16|   Flat 12 Bierwerks| Indianapolis|   IN| 16|
|  17|Tin Man Brewing C...|   Evansville|   IN| 17|
|  18|Black Acre Brewin...| Indianapolis|   IN| 18|
+----+--------------------+-------------+-----+---+
only showing top 20 rows

```

¿Qué tan genial es eso? Tu propio cluster Spark para jugar.


## Inicio Rápido Hive

Ve a la línea de comandos del servidor Hive e inicia hiveserver2

```
  docker exec -it hive-server bash

  hiveserver2
```

Quizás una pequeña verificación de que algo está escuchando en el puerto 10000 ahora
```
  netstat -anp | grep 10000
tcp        0      0 0.0.0.0:10000           0.0.0.0:*               LISTEN      446/java

```

De acuerdo. Beeline es la interfaz de línea de comandos con Hive. Conectémonos a hiveserver2 ahora.

```
  beeline -u jdbc:hive2://localhost:10000 -n root
  
  !connect jdbc:hive2://127.0.0.1:10000 scott tiger
```

No esperaba encontrar scott/tiger nuevamente después de mis días de Oracle. Pero ahí lo tienes. Definitivamente no es una buena idea mantener ese usuario en producción.

No hay muchas bases de datos aquí todavía.
```
  show databases;
  
+----------------+
| database_name  |
+----------------+
| default        |
+----------------+
1 row selected (0.335 seconds)
```

Cambiemos eso.

```
  create database openbeer;
  use openbeer;
```

Y creemos una tabla.

```
CREATE EXTERNAL TABLE IF NOT EXISTS breweries(
    NUM INT,
    NAME CHAR(100),
    CITY CHAR(100),
    STATE CHAR(100),
    ID INT )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/data/openbeer/breweries';
```

Y hagamos una pequeña declaración select.

```
  select name from breweries limit 10;
+----------------------------------------------------+
|                        name                        |
+----------------------------------------------------+
| name                                                                                                 |
| NorthGate Brewing                                                                                    |
| Against the Grain Brewery                                                                            |
| Jack's Abby Craft Lagers                                                                             |
| Mike Hess Brewing Company                                                                            |
| Fort Point Beer Company                                                                              |
| COAST Brewing Company                                                                                |
| Great Divide Brewing Company                                                                         |
| Tapistry Brewing                                                                                     |
| Big Lake Brewing                                                                                     |
+----------------------------------------------------+
10 rows selected (0.113 seconds)
```

Ahí lo tienes: tu servidor Hive privado para jugar.


## Configurar Variables de Entorno

Los parámetros de configuración se pueden especificar en el archivo hadoop.env o como variables ambientales para servicios específicos (por ejemplo, namenode, datanode, etc.):
```
  CORE_CONF_fs_defaultFS=hdfs://namenode:8020
```

CORE_CONF corresponde a core-site.xml. fs_defaultFS=hdfs://namenode:8020 se transformará en:
```
  <property><name>fs.defaultFS</name><value>hdfs://namenode:8020</value></property>
```
Para definir un guión dentro de un parámetro de configuración, usa triple guion bajo, como YARN_CONF_yarn_log___aggregation___enable=true (yarn-site.xml):
```
  <property><name>yarn.log-aggregation-enable</name><value>true</value></property>
```

Las configuraciones disponibles son:
* /etc/hadoop/core-site.xml CORE_CONF
* /etc/hadoop/hdfs-site.xml HDFS_CONF
* /etc/hadoop/yarn-site.xml YARN_CONF
* /etc/hadoop/httpfs-site.xml HTTPFS_CONF
* /etc/hadoop/kms-site.xml KMS_CONF
* /etc/hadoop/mapred-site.xml  MAPRED_CONF

Si necesitas extender algún otro archivo de configuración, consulta el script bash base/entrypoint.sh.


sudo systemctl start docker
docker-compose build clima  
docker-compose up -d  
docker-compose logs -f clima