from .base import *

test_env = Env()
test_env.read_env(os.path.join(BASE_DIR, '.env.testing'), override=True)

DB_CONF = test_env.json('DATABASE_CONF')

ACTIVEPLANT_CONF = test_env.json('PLANT_CONF')

API_CONFIG = test_env.json('API_CONFIG')

# MONSOL = test_env.json('MONSOL')

# MONSOL_CREDENTIALS = test_env.json('MONSOL_CREDENTIALS')