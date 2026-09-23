import os
# 1. Configuración dinámica del entorno
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

# 2. Inicializar la SparkSession con 2 núcleos y nombre de aplicación
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("MiPrimeraAplicacionSpark") \
    .getOrCreate()

print("\n" + "="*40)
print(f"Sesión activa: {spark}")
print("="*40 + "\n")

sc = spark.sparkContext          # la puerta de entrada a los RDD

rdd = sc.parallelize(range(1, 13), 4)
rdd.getNumPartitions()           # 4
rdd.glom().collect()             # muestra qué hay dentro de cada partición
print(rdd.map(lambda x: x * 2).toDebugString().decode())  # imprime el linaje

rdd_1=sc.parallelize([1233,3344,122222])
rdd_2=rdd_1.map(lambda x: x*2) #DF

print("="*40)
print(rdd_2.collect())
print("="*40)

# Convertir de RDD a DF
df_numeros=rdd_2.map(lambda x:(x,)).toDF(["valor_multiplicado"])
df_numeros.show()

df_numeros_2=spark.createDataFrame(rdd_1, IntegerType())
df_numeros_2=df_numeros_2.withColumnRenamed("value","valor")
df_numeros_2.show()

# EJERCICIO 1 - CLASE 3
# Cada línea: vuelo, origen, destino, cabina, precio_usd
vuelos = [
    "LA3021,BOG,MDE,economy,95",
    "LA3021,BOG,MDE,premium,210",
    "LA2410,LIM,BOG,economy,180",
    "LA800,SCL,LIM,economy,150",
    "LA2410,LIM,BOG,economy,175",
    "LA4040,BOG,CTG,economy,110",
    "LA800,SCL,LIM,premium,390",
    "LA4040,BOG,CTG,economy,120",
    "LA3021,BOG,MDE,economy,90",
    "LA1500,UIO,BOG,economy,160",
]

comentarios = [
    "vuelo puntual y comida buena",
    "vuelo retrasado y comida fria",
    "tripulacion amable vuelo puntual",
    "asiento incomodo pero tripulacion amable",
]

rdd_vuelos=sc.parallelize(vuelos,5)

print("="*40)
print(rdd_vuelos.collect())
print("="*40)
print(f'Conteo: {rdd_vuelos.count()}')
print("="*40)

rdd_campos=rdd_vuelos.map(lambda x: x.split(','))

print("="*40)
print(rdd_campos.collect())
print("="*40)
print(f'Conteo: {rdd_campos.count()}')
print("="*40)

rdd_pasajes=rdd_campos.map(lambda x: (x[0],x[1],x[2],x[3],int(x[4])))

print("="*40)
print(rdd_pasajes.collect())
print("="*40)
print(f'Conteo: {rdd_pasajes.count()}')
print("="*40)

ciudades=rdd_pasajes.flatMap(lambda x: [x[1],x[2]]).distinct()
print("="*40)
print(ciudades.collect())
print("="*40)
print(f'Conteo: {ciudades.count()}')
print("="*40)

# 3. Detener ordenadamente la sesión
spark.stop()