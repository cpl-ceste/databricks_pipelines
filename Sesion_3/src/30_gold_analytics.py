import sys, os

# Ajusta la ruta según tu repositorio en Databricks
sys.path.append("/Workspace/Users/psanzc@hotmail.com/optimizacion_databricks/Sesion_3/")

from utils.utils_config import load_config
from utils.utils_io import save_table
from pyspark.sql.functions import avg

cfg = load_config()

flights = spark.table(f"{cfg['catalog']}.{cfg['schemas']['silver']}.flights_enriched")

agg = flights.groupBy("origin_city").agg(avg("delay").alias("avg_delay"))

save_table(agg, cfg["catalog"], cfg["schemas"]["gold"], "delay_summary")
