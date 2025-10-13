import json
import os

def load_config(env: str = None):
    if env is None:
        env = os.environ.get("ENV", "dev")
    with open(f"../conf/config.{env}.json") as f:
        return json.load(f)
