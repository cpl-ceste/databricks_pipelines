import sys, os

# Ajusta la ruta según tu repositorio en Databricks
sys.path.append("/Workspace/Users/psanzc@hotmail.com/optimizacion_databricks/Sesion_3/")

from utils.utils_config import load_config
from utils.utils_quality import check_not_null

cfg = load_config()

flights = spark.table(f"{cfg['catalog']}.{cfg['schemas']['silver']}.flights_enriched")

if not check_not_null(flights, "delay"):
    raise Exception("Quality check failed: delay column has NULLs")
