from .base import *

test_env = Env()
test_env.read_env(os.path.join(BASE_DIR, '.env.testing'), override=True)

DB_CONF = test_env.json('DATABASE_CONF')
