from pyspark.sql import SparkSession

# Inicializa SparkSession con soporte para Hive
spark = SparkSession.builder \
    .appName("CargarParquetEnHive") \
    .enableHiveSupport() \
    .getOrCreate()

# Rutas a tus carpetas Parquet (ajusta según la ubicación real)
lote1_path = "/app/data/Visitas_lote_01_kpi_parquet"
lote2_path = "/app/data/Visitas_lote_02_parquet_kpi_parquet"

# Lee cada lote y crea una tabla Hive
print("Cargando lote 1...")
df1 = spark.read.parquet(lote1_path)
df1.write.mode("overwrite").saveAsTable("kpi_lote_01")
print("Lote 1 cargado en Hive como tabla: kpi_lote_01")

print("Cargando lote 2...")
df2 = spark.read.parquet(lote2_path)
df2.write.mode("overwrite").saveAsTable("kpi_lote_02")
print("Lote 2 cargado en Hive como tabla: kpi_lote_02")

print("Proceso terminado correctamente")

spark.stop()