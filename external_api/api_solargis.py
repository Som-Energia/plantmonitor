import requests

import datetime

from conf.log import logger

from typing import NamedTuple

class Site(NamedTuple):
    id: str
    name: str
    lat: float
    long: float

class ApiSolargis:

    def __init__(self, api_base_url, api_key):

        self.api_base_url = api_base_url
        self.api_request_url = "{}/{}".format(api_base_url, "datadelivery/request")
        self.api_key = api_key

        self.api_key_params = {'key': api_key}

    def get_arbitrary_payload(self, xml_request_content):
        headers = {'Content-Type' : 'application/xml'}

        response = requests.post(self.api_request_url, params=self.api_key_params, headers=headers, data=xml_request_content.encode('utf8'))

        if response.status_code != 200:
            logger.error(response.text)
            return response.status_code, None

        text_response = response.text
        return response.status_code, text_response

    def text_response_to_readings(self, text_response):

        import pdb; pdb.set_trace()


        return

    def get_current_solargis_irradiance_readings(self, site: Site, from_date, to_date):

        from_date_str = datetime.datetime.strftime(from_date, '%Y-%m-%d')
        to_date_str = datetime.datetime.strftime(to_date, '%Y-%m-%d')

        # TODO add device as optional value
        payload = {
        }

        xml_request_content = f'''
            <ws:dataDeliveryRequest dateFrom="{from_date_str}" dateTo="{to_date_str}"
            xmlns="http://geomodel.eu/schema/data/request"
            xmlns:ws="http://geomodel.eu/schema/ws/data"
            xmlns:geo="http://geomodel.eu/schema/common/geo"
            xmlns:pv="http://geomodel.eu/schema/common/pv"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <site id="{site.id}" name="{site.name}" lat="{site.lat}" lng="{site.long}">
            </site>
            <processing key="GHI" summarization="HOURLY" terrainShading="true">
            </processing>
            </ws:dataDeliveryRequest>
        '''

        logger.debug(xml_request_content)

        # TODO we might be able to use stream=True with a context manager for partially reading big bodies

        text_response = self.get_arbitrary_payload(xml_request_content)

        readings = self.text_response_to_readings(text_response)

        return readings
