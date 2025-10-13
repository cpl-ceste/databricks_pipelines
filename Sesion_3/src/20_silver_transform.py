import sys, os

# Ajusta la ruta según tu repositorio en Databricks
sys.path.append("/Workspace/Users/psanzc@hotmail.com/optimizacion_databricks/Sesion_3/")

from utils.utils_config import load_config
from utils.utils_io import save_table
from pyspark.sql.functions import col

cfg = load_config()

flights = spark.table(f"{cfg['catalog']}.{cfg['schemas']['bronze']}.flights")
airports = spark.table(f"{cfg['catalog']}.{cfg['schemas']['bronze']}.airports")

flights_clean = flights.withColumn("delay", col("delay").cast("int"))

flights_enriched = flights_clean.join(
    airports, flights_clean.origin == airports.airport_code, "left"
).withColumnRenamed("city", "origin_city")

save_table(flights_enriched, cfg["catalog"], cfg["schemas"]["silver"], "flights_enriched")
