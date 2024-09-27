from .base import BASE_DIR, Env
import os

prod_env = Env()
prod_env.read_env(os.path.join(BASE_DIR, ".env.prod"), override=True)

# DATABASE configuration
DB_CONF = prod_env.json("DATABASE_CONF")

ACTIVEPLANT_CONF = prod_env.json("PLANT_CONF")

API_CONFIG = prod_env.json("API_CONFIG")

# MONSOL = prod_env.json('MONSOL')
# MONSOL_CREDENTIALS = prod_env.json('MONSOL_CREDENTIALS')

SOLARGIS = prod_env.json("SOLARGIS")
