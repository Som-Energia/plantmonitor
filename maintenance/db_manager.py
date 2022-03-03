from sqlalchemy import create_engine
from sqlalchemy.orm import Session

class DBManager():

    def __init__(self, user, host, port, dbname, password=None, echo=False):

        userpass = '{}:{}'.format(user, password) if password else user

        self.engine_str = 'postgresql+psycopg2://{}@{}:{}/{}'.format(userpass, host, port, dbname)

        self.echo = echo

        self.alchemy_engine = create_engine(self.engine_str, pool_recycle=3600, echo=self.echo)
        self.db_con = self.alchemy_engine.connect()

    def close_db(self):
        self.db_con.close()
        self.alchemy_engine.dispose()

    def create_session(self):
        return Session(self.alchemy_engine)

    def insert_db(self, df, tablename):
        with self as dbman:
            df.to_sql(tablename, dbman.db_con)