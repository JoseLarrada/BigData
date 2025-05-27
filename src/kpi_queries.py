from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, date_format, to_date


base_path = "datos_parquet/"

# Crear sesión de Spark con soporte Hive
spark = SparkSession.builder \
    .appName("Unir Lotes KPI y Modelado Dimensional") \
    .enableHiveSupport() \
    .getOrCreate()

# Unir lotes
lote1 = spark.table("kpi_lote_01")
lote2 = spark.table("kpi_lote_02")
kpi_union = lote1.unionByName(lote2)

# Guardar tabla unificada
kpi_union.write.mode("overwrite").saveAsTable("kpi_total_unificado")
print("✅ Tabla unificada creada: kpi_total_unificado")

# Cargar tabla unificada
df = spark.table("kpi_total_unificado")

# =====================================
# Tabla de Hechos: hechos_visitas
# =====================================
hechos_visitas = df.select(
    "visitId",
    "fullVisitorId",
    "date",
    "visitNumber",
    "visitStartTime",
    "visits",
    "pageviews",
    "transactions",
    "transactionRevenue",
    "channelGrouping",
    "hitType"
)
hechos_visitas.write.mode("overwrite").saveAsTable("hechos_visitas")
hechos_visitas.coalesce(1).write.mode("overwrite").parquet(base_path + "hechos_visitas")
# =====================================
# Dimensión Tiempo
# =====================================
dim_tiempo = df.select("date").distinct() \
    .withColumn("date", to_date(col("date").cast("string"), "yyyyMMdd")) \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .withColumn("day", dayofmonth(col("date"))) \
    .withColumn("weekday", date_format(col("date"), "EEEE"))

dim_tiempo.write.mode("overwrite").saveAsTable("dim_tiempo")
dim_tiempo.coalesce(1).write.mode("overwrite").parquet(base_path + "dim_tiempo")

# =====================================
# Dimensión Visitante
# =====================================
dim_visitante = df.select("fullVisitorId", "socialEngagementType").distinct()
dim_visitante.write.mode("overwrite").saveAsTable("dim_visitante")
dim_visitante.coalesce(1).write.mode("overwrite").parquet(base_path + "dim_visitante")

# =====================================
# Dimensión Canal
# =====================================
dim_canal = df.select("channelGrouping").distinct()
dim_canal.write.mode("overwrite").saveAsTable("dim_canal")
dim_canal.coalesce(1).write.mode("overwrite").parquet(base_path + "dim_canal")

# =====================================
# Dimensión Página
# =====================================
dim_pagina = df.select(
    col("pagePath").alias("url_pagina"),
    col("referer").alias("pagina_referida"),
    "isInteraction"
).distinct()
dim_pagina.write.mode("overwrite").saveAsTable("dim_pagina")
dim_pagina.coalesce(1).write.mode("overwrite").parquet(base_path + "dim_pagina")

print("✅ Tablas derivadas creadas exitosamente")

# =============================
# Mostrar conteo de registros
# =============================

print("🔍 Conteo de registros por tabla:")
print(f"👉 kpi_total_unificado: {df.count()}")
print(f"👉 hechos_visitas: {hechos_visitas.count()}")
print(f"👉 dim_tiempo: {dim_tiempo.count()}")
print(f"👉 dim_visitante: {dim_visitante.count()}")
print(f"👉 dim_canal: {dim_canal.count()}")
print(f"👉 dim_pagina: {dim_pagina.count()}")