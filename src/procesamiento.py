from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("EjemploPySpark") \
    .getOrCreate()

# Crear un DataFrame de prueba
datos = [
    ("José", 20),
    ("Ana", 25),
    ("Luis", 30)
]
columnas = ["Nombre", "Edad"]

df = spark.createDataFrame(datos, columnas)

# Mostrar el DataFrame
df.show()

# Calcular el promedio de edad
df.selectExpr("avg(Edad) as PromedioEdad").show()

# Finalizar la sesión
spark.stop()
