import typer
import pandas as pd
import sqlalchemy
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
app = typer.Typer()

@app.command()
def csv_to_sqltable(
        csvpath: str = typer.Option(None, help='Path of origin csv'),
        dbapi: str = typer.Option(None, help='DBApi of target DB'),
        schema: str = typer.Option(None, help='Schema of target DB'),
        table: str = typer.Option(None, help='table name of target DB'),
        ifexists: str = typer.Option(None, help='Append or replace'),
        truncate: bool = typer.Option(False,help='Truncate table before insert (TRUE or FALSE)')
    ):
    logging.info(f"Let's insert a CSV")
    dbEngine = sqlalchemy.create_engine(dbapi)
    conn = dbEngine.connect()
    logging.info(f"DB connection succesfully to {dbEngine.url.database}")
    df = pd.read_csv(csvpath)
    if truncate:
        truncquery = f'truncate table {schema}.{table};'
        conn.execute(truncquery)
    df.to_sql(table, con=conn, schema=schema, if_exists=ifexists, index=False)
    logging.info(f"CSV inserted")

if __name__ == '__main__':
    app()
