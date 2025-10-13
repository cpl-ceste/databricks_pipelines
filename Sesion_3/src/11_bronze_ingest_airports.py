import sys, os

# Ajusta la ruta según tu repositorio en Databricks
sys.path.append("/Workspace/Users/psanzc@hotmail.com/optimizacion_databricks/Sesion_3/")

from utils.utils_config import load_config
from utils.utils_io import save_table

cfg = load_config()
airports = spark.read.option("header", True).csv(cfg["data_path"] + "sample_airports.csv")

save_table(airports, cfg["catalog"], cfg["schemas"]["bronze"], "airports")
