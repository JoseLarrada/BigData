from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, approx_count_distinct, count, min, max, avg
import sys

if len(sys.argv) != 2:
    print("Uso: python estadisticas_parquet_optimizada.py <ruta_parquet>")
    sys.exit(1)

parquet_path = sys.argv[1]
spark = SparkSession.builder \
    .appName("EstadisticasColumnaAColumnaOpt") \
    .config("spark.sql.parquet.columnarReaderBatchSize", "256") \
    .getOrCreate()

df = spark.read.parquet(parquet_path)
columnas = df.columns

print(f"\nColumnas detectadas: {columnas}\n")
MUESTRA = 0.01  # 1% de la data

for c in columnas:
    print(f"=== Columna: {c} ===")
    # Trabaja sobre una muestra pequeña
    dcol = spark.read.parquet(parquet_path).select(c).sample(False, MUESTRA, seed=42)
    row_count = dcol.count()
    if row_count == 0:
        print("Sin datos en la muestra. Saltando columna.")
        continue

    nulos = dcol.filter(col(c).isNull() | isnan(col(c))).count()
    print(f"Filas en muestra: {row_count}")
    print(f"Valores nulos/NaN: {nulos} ({(nulos/row_count*100):.2f}%)")
    unicos = dcol.select(approx_count_distinct(col(c))).collect()[0][0]
    print(f"Valores únicos en muestra (aprox): {unicos}")
    tipo = dict(dcol.dtypes)[c]

    if tipo in ["int", "bigint", "double", "float", "long"]:
        stats = dcol.agg(min(c).alias("min"), max(c).alias("max"), avg(c).alias("mean")).collect()[0]
        print(f"Min: {stats['min']} | Max: {stats['max']} | Promedio: {stats['mean']}")
    elif tipo == "string" and unicos <= 100:
        print("Top 5 valores más frecuentes:")
        dcol.groupBy(c).count().orderBy("count", ascending=False).show(5, truncate=30)
    else:
        print("Columna string con muchos valores únicos: omitiendo top frecuentes.")
    print("-" * 40)

spark.stop()