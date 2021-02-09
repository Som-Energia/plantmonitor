from .base import *

devel_env = Env()
devel_env.read_env(os.path.join(BASE_DIR, '.env.devel'), override=True)

# DATABASE configuration
DB_CONF = devel_env.json('DATABASE_CONF')
