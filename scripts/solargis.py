import datetime

import typer

from typing import List, Optional

from conf.log import logger

from external_api.api_solargis import ApiSolargis

import urllib.parse

app = typer.Typer()


def dbapi_to_dict(dbapi: str):
    parsed_string = urllib.parse.urlparse(dbapi)

    return {
        "provider": parsed_string.scheme,
        "user": parsed_string.username,
        "password": (
            urllib.parse.unquote(parsed_string.password)
            if parsed_string.password
            else None
        ),
        "host": parsed_string.hostname,
        "port": parsed_string.port,
        "database": parsed_string.path[1:],
    }


@app.command()
def download_save_solargis_readings(
    dbapi: str = typer.Argument(
        ...,
        help="Database connection. Has to be single-quoted if it contains special characters.",
    ),
    solargis_api_key: str = typer.Argument(
        ...,
        help="Solargis API key",
    ),
    from_date: datetime.datetime = typer.Argument(
        ...,
        help="Start date in format YYYY-MM-DD",
    ),
    to_date: datetime.datetime = typer.Argument(
        ...,
        help="End date in format YYYY-MM-DD",
    ),
    plant_ids: Optional[List[int]] = typer.Argument(
        None,
        help="List of plant IDs",
    ),
):
    """
    Download and save Solargis readings to a database.
    """
    from_date = from_date.replace(tzinfo=datetime.timezone.utc)
    to_date = to_date.replace(tzinfo=datetime.timezone.utc)

    solargis_conf = {
        "api_base_url": "https://solargis.info/ws/rest",
        "api_key": solargis_api_key,
    }

    database_info = dbapi_to_dict(dbapi)

    logger.info(
        f"Downloading sites {plant_ids} from {from_date} to {to_date} to {database_info['host']}/{database_info['database']}"
    )

    # TODO parametrize processing_keys
    processing_keys = "GHI GTI TMOD PVOUT"
    num_rows = ApiSolargis.download_readings(
        solargis_conf, database_info, from_date, to_date, processing_keys, plant_ids
    )
    logger.info(f"Inserted {num_rows} rows.")

    logger.info("Job's Done, Have A Nice Day.")


if __name__ == "__main__":
    app()
