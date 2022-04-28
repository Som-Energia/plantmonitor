import requests

import datetime

import xmlschema

from conf.log import logger

from plantmonitor.utils import rfc3336todt

class ApiSolargis:

    def __init__(self, api_base_url, site_id, api_key):

        self.api_base_url = api_base_url
        self.api_request_url = "{}/{}".format(api_base_url, "datadelivery/request")
        self.site_id = site_id
        self.api_key = api_key

        self.api_key_params = {'key': api_key}

    def create_xsd_schema(self):
        self.schema = xmlschema.XMLSchema('data/ws-data.xsd')

    def get_arbitrary_payload(self, xml_request_content):
        headers = {'Content-Type' : 'application/xml'}

        response = requests.post(self.api_request_url, params=self.api_key_params, headers=headers, data=xml_request_content.encode('utf8'))

        if response.status_code != 200:
            logger.error(response.text)
            return response.status_code, None

        text_response = response.text
        return response.status_code, text_response

    def text_response_to_readings(self, text_response):

        response_dict = self.schema.to_dict(text_response)
        readings_dirty = response_dict['site'][0]['row']
        readings = [
            (rfc3336todt(v['@dateTime']), *v['@values'])
            for v in readings_dirty
        ]
        return readings

    def get_current_solargis_irradiance_readings(self, lat, long, from_date, to_date, processing_keys):

        from_date_str = datetime.datetime.strftime(from_date, '%Y-%m-%d')
        to_date_str = datetime.datetime.strftime(to_date, '%Y-%m-%d')

        xml_request_content = f'''
            <ws:dataDeliveryRequest dateFrom="{from_date_str}" dateTo="{to_date_str}"
            xmlns="http://geomodel.eu/schema/data/request"
            xmlns:ws="http://geomodel.eu/schema/ws/data"
            xmlns:geo="http://geomodel.eu/schema/common/geo"
            xmlns:pv="http://geomodel.eu/schema/common/pv"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <site id="{self.site_id}" lat="{lat}" lng="{long}">
            </site>
            <processing key="{processing_keys}" summarization="HOURLY" terrainShading="true">
            </processing>
            </ws:dataDeliveryRequest>
        '''

        logger.debug(xml_request_content)

        status, text_response = self.get_arbitrary_payload(xml_request_content)

        readings = self.text_response_to_readings(text_response) if status == 200 else None

        return status, readings