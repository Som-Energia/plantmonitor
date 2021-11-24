import requests

from conf.logging_configuration import LOGGING

import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")


class ApiMonsol:

    def __init__(self, config):
        api_base_url = config['api_base_url']
        self.api_auth_url = config['api_auth_url']
        version = config['version']
        self.username = config['username']
        self.password = config['password']

        self.api_url = '{}/{}'.format(api_base_url, version)

        self.token = ''

    def get_token(self):
        url = '{}/{}'.format(self.api_auth_url,'token.php')

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {'client_id':self.username, 'client_secret': self.password}

        response = requests.post(url, headers=headers, data=data)

        logger.debug(response.json())

        status = response.status_code

        if status != 200:
            logger.error(response.json())
            return status, None

        access_token = response.json()['access_token']

        self.token = access_token

        return status, self.token

    def reuse_token(self):
        # TODO doesn't handle the expiration of token

        if self.token:
            return self.token

        return self.get_token()

    def split_date(self, date):

        fecha = date.strftime("%Y-%m-%d")
        hora = date.strftime("%H:%M")

        return fecha, hora

    def get_meter(self, date_from=None, date_to=None, granularity=None, device=None):

        # TODO handle None for date_from and date_to
        rgranularity = granularity or 60

        fecha_ini, hora_ini = self.split_date(date_from)
        fecha_fin, hora_fin = self.split_date(date_to)

        url = '{}/{}'.format(self.api_url,'obtenerDatosActual/3')

        headers = {'Authorization' : 'Bearer {}'.format(self.token)}
        # TODO add device as optional value
        payload = {
            'fecha_ini': fecha_ini,
            'hora_ini': hora_ini,
            'fecha_fin': fecha_fin,
            'hora_fin': hora_fin,
            'intervaloHorario': rgranularity
        }

        response = requests.post(url, headers=headers, json=payload)
        if response.status_code != 200:
            logger.error(response.json())
            return response.status_code, None

        readings = response.json()

        return response.status_code, readings
