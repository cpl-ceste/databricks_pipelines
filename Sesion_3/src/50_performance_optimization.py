import sys, os

# Ajusta la ruta según tu repositorio en Databricks
sys.path.append("/Workspace/Users/psanzc@hotmail.com/optimizacion_databricks/Sesion_3/")

from utils.utils_config import load_config
from pyspark.sql.functions import col, broadcast

cfg = load_config()

# 1. Pipeline lento: join sin partición
flights = spark.table(f"{cfg['catalog']}.{cfg['schemas']['bronze']}.flights")
airports = spark.table(f"{cfg['catalog']}.{cfg['schemas']['bronze']}.airports")

# join inicial (no optimizado)
joined = flights.join(airports, flights.origin == airports.airport_code)
print(f"Registros: {joined.count()}")

# 2. Diagnóstico: plan de ejecución
joined.explain(True)

# 3. Optimización: broadcast join + cache + AQE
# spark.conf.set("spark.sql.adaptive.enabled", True)
optimized = flights.join(broadcast(airports), flights.origin == airports.airport_code)
# optimized.cache()
print(f"Registros optimizados: {optimized.count()}")
