from sqlalchemy import create_engine
from sqlalchemy.orm import Session

class DBManager():

    def __init__(self, user, host, port, dbname, provider=None, password=None):

        userpass = '{}:{}'.format(user, password) if password else user

        self.engine_str = 'postgresql+psycopg2://{}@{}:{}/{}'.format(userpass, host, port, dbname)

    def __enter__(self):
        self.alchemy_engine = create_engine(self.engine_str, pool_recycle=3600)
        self.db_con = self.alchemy_engine.connect()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.db_con.close()
        self.alchemy_engine.dispose()

    def create_session(self):
        return Session(self.alchemy_engine)