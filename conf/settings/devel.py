from .base import *

devel_env = Env()
devel_env.read_env(os.path.join(BASE_DIR, '.env.devel'), override=True)

# DATABASE configuration
DB_CONF = devel_env.json('DATABASE_CONF')

ACTIVEPLANT_CONF = devel_env.json('PLANT_CONF')

API_CONFIG = devel_env.json('API_CONFIG')

# MONSOL = devel_env.json('MONSOL')
# MONSOL_CREDENTIALS = devel_env.json('MONSOL_CREDENTIALS')

SOLARGIS = devel_env.json('SOLARGIS')