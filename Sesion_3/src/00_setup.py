import sys
import os
import json

# Ensure the correct path to the configuration file
config_path = '/Workspace/Users/psanzc@hotmail.com/sesion_3/conf/config.dev.json'
if not os.path.exists(config_path):
    raise FileNotFoundError(f"No such file or directory: '{config_path}'")

with open(config_path, 'r') as file:
    cfg = json.load(file)


spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg['catalog']}")
for schema in cfg['schemas'].values():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg['catalog']}.{schema}")
