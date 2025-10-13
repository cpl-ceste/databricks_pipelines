import sys, os

# Ajusta la ruta según tu repositorio en Databricks
sys.path.append("/Workspace/Users/psanzc@hotmail.com/optimizacion_databricks/Sesion_3/")


from utils.utils_config import load_config
from utils.utils_io import save_table

cfg = load_config()
data_path = cfg["data_path"] + "sample_flights.csv"
flights = spark.read.option("header", True).csv(data_path)

save_table(flights, cfg["catalog"], cfg["schemas"]["bronze"], "flights")
