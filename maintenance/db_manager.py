import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

class DBManager():

    def __init__(self, provider, user, host, port, database, password=None, echo=False):

        user_encoded = urllib.parse.quote_plus(user)
        password_encoded = urllib.parse.quote_plus(password) if password else None

        userpass = '{}:{}'.format(user_encoded, password_encoded) if password_encoded else user_encoded

        self.engine_str = '{}+psycopg2://{}@{}:{}/{}'.format(provider+'ql',userpass, host, port, database)

        self.echo = echo

        self.alchemy_engine = create_engine(self.engine_str, pool_recycle=3600, echo=self.echo)
        self.db_con = self.alchemy_engine.connect()

    def __enter__(self):
        #just use default constructor
        return self

    def __exit__(self, *args, **kwargs):
        self.close_db()

    def close_db(self):
        self.db_con.close()
        self.alchemy_engine.dispose()

    # TODO remove this, use `with self.db_con.begin()` instead
    def create_session(self):
        return Session(self.alchemy_engine)

    def insert_db(self, df, tablename):
        with self as dbman:
            df.to_sql(tablename, dbman.db_con)