from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, when
from pyspark.sql.types import MapType, StringType, ArrayType
import os, shutil

spark = SparkSession.builder \
    .appName("LimpiezaVisitasParquet") \
    .master("local[4]") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .getOrCreate()

path_visitas1 = "data/Visitas_lote_01.csv"
path_visitas2 = "data/Visitas_lote_02.csv"

parquet_dir1 = "/app/muestras/muestra_lote_01_parquet"
parquet_dir2 = "/app/muestras/muestra_lote_02_parquet"

sample_csv_dir1 = "/app/muestras/muestra_lote_01_csv"
sample_csv_dir2 = "/app/muestras/muestra_lote_02_csv"

device_schema = MapType(StringType(), StringType())
custom_dimensions_schema = ArrayType(MapType(StringType(), StringType()))

def limpiar_df(df):
    df = df.withColumn("device", to_json(from_json(col("device"), device_schema))) \
           .withColumn("customDimensions", to_json(from_json(col("customDimensions"), custom_dimensions_schema)))

    exprs = []
    for c, t in df.dtypes:
        if t == 'string':
            exprs.append(when(col(c).contains("not available in demo dataset"), None).otherwise(col(c)).alias(c))
        else:
            exprs.append(col(c))

    return df.select(*exprs)

def mover_csv_final(ruta_origen, nombre_final):
    for archivo in os.listdir(ruta_origen):
        if archivo.endswith(".csv"):
            origen = os.path.join(ruta_origen, archivo)
            destino = os.path.join(ruta_origen, f"{nombre_final}.csv")
            shutil.move(origen, destino)
            print(f"✔ CSV generado: {nombre_final}.csv")
            return

# Leer CSVs originales
df1 = spark.read.option("header", True).option("multiLine", True).option("escape", "\"")\
    .csv(path_visitas1)
df2 = spark.read.option("header", True).option("multiLine", True).option("escape", "\"")\
    .option("maxCharsPerColumn", 10000000).option("maxColumns", 10000)\
    .csv(path_visitas2)

# Limpiar DataFrames
df1_clean = limpiar_df(df1).repartition(64)
df2_clean = limpiar_df(df2).repartition(128)

# Guardar en formato Parquet (optimizado)
df1_clean.write.mode("overwrite").parquet(parquet_dir1)
df2_clean.write.mode("overwrite").parquet(parquet_dir2)

print("✔ Parquet guardado correctamente.")

spark.stop()