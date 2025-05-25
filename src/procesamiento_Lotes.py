from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json, udf
from pyspark.sql.types import *
import glob
import ast
import os

# Esquema para totals
totals_schema = StructType([
    StructField("visits", StringType()),
    StructField("pageviews", StringType()),
    StructField("transactions", StringType()),
    StructField("transactionRevenue", StringType())
])

columnas_relevantes = [
    "fullVisitorId", "visitId", "visitNumber", "date", "visitStartTime",
    "channelGrouping", "socialEngagementType", "totals", "hits"
]

def extraer_hit_kpi(hits_str):
    try:
        hits = ast.literal_eval(hits_str) if hits_str else []
        if isinstance(hits, dict):
            hits = [hits]
        if not isinstance(hits, list) or not hits:
            return (None, None, None, None)
        h = hits[0]
        hit_type = h.get("type")
        page_path = h.get("page", {}).get("pagePath")
        is_interaction = h.get("isInteraction")
        referer = h.get("referer")
        return (hit_type, page_path, is_interaction, referer)
    except Exception:
        return (None, None, None, None)

schema_kpi = StructType([
    StructField("hitType", StringType()),
    StructField("pagePath", StringType()),
    StructField("isInteraction", StringType()),
    StructField("referer", StringType())
])

extraer_kpi_udf = udf(extraer_hit_kpi, schema_kpi)

def limpiar_y_agrupar_visitas(df):
    for c in ["device", "geoNetwork", "trafficSource"]:
        if c in df.columns:
            df = df.drop(c)
    cols_presentes = [c for c in columnas_relevantes if c in df.columns]
    df = df.select(*cols_presentes)
    for c, t in df.dtypes:
        if t == 'string':
            df = df.withColumn(c, when(col(c).contains("not available in demo dataset"), None).otherwise(col(c)))
    if dict(df.dtypes).get('totals') == 'string':
        df = df.withColumn("totals_json", from_json(col("totals"), totals_schema))
        df = df.withColumn("visits", col("totals_json.visits").cast("int")) \
               .withColumn("pageviews", col("totals_json.pageviews").cast("int")) \
               .withColumn("transactions", col("totals_json.transactions").cast("int")) \
               .withColumn("transactionRevenue", col("totals_json.transactionRevenue").cast("double")) \
               .drop("totals").drop("totals_json")
    else:
        for f in ['visits', 'pageviews', 'transactions', 'transactionRevenue']:
            if f in df.columns:
                df = df.withColumn(f, col(f).cast("double" if f == "transactionRevenue" else "int"))
    df = df.withColumn("kpi", extraer_kpi_udf(col("hits")))
    df = df.withColumn("hitType", col("kpi.hitType")) \
           .withColumn("pagePath", col("kpi.pagePath")) \
           .withColumn("isInteraction", col("kpi.isInteraction")) \
           .withColumn("referer", col("kpi.referer")) \
           .drop("kpi").drop("hits")
    columnas_finales = [
        "fullVisitorId", "date", "visitId", "visitNumber", "visitStartTime", "channelGrouping",
        "socialEngagementType", "visits", "pageviews", "transactions", "transactionRevenue",
        "hitType", "pagePath", "isInteraction", "referer"
    ]
    columnas_finales = [c for c in columnas_finales if c in df.columns]
    df = df.dropna(subset=["fullVisitorId", "visitId"])
    df = df.select(*columnas_finales)
    return df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Limpieza KPIs desde Parquet por lotes") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "128") \
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "128") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .getOrCreate()

    # Ajusta aquí el nombre de tu carpeta de entrada y salida
    carpeta_entrada = "data/Visitas_lote_02_parquet"
    carpeta_salida = "data/Visitas_lote_02_parquet_kpi_parquet"
    batch_size = 1

    # Limpiar carpeta de salida si existe (opcional, solo la primera vez)
    if os.path.exists(carpeta_salida):
        import shutil
        shutil.rmtree(carpeta_salida)

    parquet_files = sorted(glob.glob(f"{carpeta_entrada}/*.parquet"))
    total_files = len(parquet_files)
    print(f"Procesando {total_files} archivos en lotes de {batch_size}...")

    for i in range(0, total_files, batch_size):
        batch_files = parquet_files[i:i+batch_size]
        print(f"[{i+1}-{min(i+batch_size, total_files)}] Procesando archivos: {batch_files}")
        df = spark.read.parquet(*batch_files)
        df = df.repartition(1)
        df_kpi = limpiar_y_agrupar_visitas(df)
        mode = "append" if i != 0 else "overwrite"
        df_kpi.write.mode(mode).parquet(carpeta_salida)
        print(f"✅ Lote {i//batch_size+1} procesado y guardado en {carpeta_salida}")

    print("🎉 ¡Listo! Todos los archivos Parquet procesados por lotes.")