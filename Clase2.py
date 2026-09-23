import os
import socket
import getpass

# Definir la ruta de Java 17
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

import findspark
findspark.init()

from pyspark.sql import SparkSession

# Obtener datos del entorno
nombre_maquina = socket.gethostname()
usuario_actual = getpass.getuser()

# Crear una sesión local de Spark
spark = (
    SparkSession.builder
    .appName("ValidacionNetec")
    .master("local[*]")
    .getOrCreate()
)

print("\n" + "=" * 40)
print(f"Versión de Spark detectada: {spark.version}")
print("=" * 40 + "\n")

# Crear los datos de prueba
datos = [
    (usuario_actual, "Usuario"),
    (nombre_maquina, "Servidor")
]

columnas = ["Nombre", "Rol"]

# Crear y mostrar el DataFrame
df = spark.createDataFrame(datos, columnas)

df.show(truncate=False)

# Finalizar la sesión de Spark
spark.stop()