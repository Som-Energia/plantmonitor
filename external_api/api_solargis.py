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
        self.api_name = 'solargis'

        self.api_key_params = {'key': api_key}

        # TODO set from database wither via Pony, sqlalchemy or else if we stick with a satellite api
        # {plant_id: (lat, long)}
        self.plant_locations = {
            3: (40.932389, -4.968694), # Fontivsolar
            9: (39.440722, -0.428722), # Picanya
            70: (37.504330, -3.236476) # Llanillos
        }

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

    def text_response_to_readings(self, text_response, request_time=None):

        request_time = request_time or datetime.datetime.now(datetime.timezone.utc)

        response_dict = self.schema.to_dict(text_response)
        readings_dirty = response_dict['site'][0]['row']
        readings = [
            (rfc3336todt(v['@dateTime']), *v['@values'], self.api_name, request_time)
            for v in readings_dirty
        ]
        return readings

    def get_current_solargis_irradiance_readings_location(self, lat, lon, from_date, to_date, processing_keys):

        from_date_str = datetime.datetime.strftime(from_date, '%Y-%m-%d')
        to_date_str = datetime.datetime.strftime(to_date, '%Y-%m-%d')

        xml_request_content = f'''
            <ws:dataDeliveryRequest dateFrom="{from_date_str}" dateTo="{to_date_str}"
            xmlns="http://geomodel.eu/schema/data/request"
            xmlns:ws="http://geomodel.eu/schema/ws/data"
            xmlns:geo="http://geomodel.eu/schema/common/geo"
            xmlns:pv="http://geomodel.eu/schema/common/pv"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <site id="{self.site_id}" lat="{lat}" lng="{lon}">
            </site>
            <processing key="{processing_keys}" summarization="HOURLY" terrainShading="true">
            </processing>
            </ws:dataDeliveryRequest>
        '''

        logger.debug(xml_request_content)

        status, text_response = self.get_arbitrary_payload(xml_request_content)

        readings = self.text_response_to_readings(text_response) if status == 200 else None

        return status, readings

    def get_current_solargis_irradiance_readings(self, from_date=None, to_date=None, processing_keys=None):

        processing_keys = processing_keys or 'GHI GTI TMOD PVOUT'
        from_date = from_date or datetime.date.today()
        to_date = to_date or datetime.date.today()

        all_readings = []
        for plant, latlong in self.plant_locations.items():
            lat, lon = latlong
            status, readings = self.get_current_solargis_irradiance_readings_location(lat, lon, from_date, to_date, processing_keys)
            if status != 200:
                logger.error(f"Error reading plant {plant} {status}")
            else:
                all_readings = [
                    (t, plant, *values)
                    for t, *values in readings
                ]

        return all_readings

    def get_current_solargis_readings_standarized(self, from_date=None, to_date=None):

        #processing_keys = 'GHI GTI TMOD PVOUT'
        #TODO TMOD doesn't work at the moment, using TEMP instead
        processing_keys = 'GHI GTI TEMP'
        from_date = from_date or datetime.date.today()
        to_date = to_date or datetime.date.today()

        all_readings = []
        for plant, latlong in self.plant_locations.items():
            lat, lon = latlong
            status, readings = self.get_current_solargis_irradiance_readings_location(lat, lon, from_date, to_date, processing_keys)
            if status != 200:
                logger.error(f"Error reading plant {plant} {status}")
            else:
                all_readings = [
                    (t, plant, int(ghi), int(gti), int(tmod*10), None, source, request_time)
                    for t, ghi, gti, tmod, source, request_time in readings
                ]

        return all_readings

    # TODO use the db_factory
    def create_table(self, db_con):
        alarm_registry = '''
            CREATE TABLE IF NOT EXISTS
            satellite_readings
            (
                time timestamptz NOT NULL,
                plant integer,
                global_horizontal_irradiation_wh_m2 bigint,
                global_tilted_irradiation_wh_m2 bigint,
                module_temperature_dc bigint,
                photovoltaic_energy_output_wh bigint,
                source text,
                request_time timestamptz,
                CONSTRAINT "fk_satellite_readings__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id") ON DELETE CASCADE
            );
            -- let's not enforce unicity for now
            --CREATE UNIQUE INDEX IF NOT EXISTS uniq_idx_time_plant_source_request_time ON satellite_readings (time, plant, source, request_time);
        '''
        db_con.execute(alarm_registry)

    def save_to_db(self, db_con, readings):

        query = '''
            INSERT INTO
            satellite_readings (
                time,
                plant,
                global_horizontal_irradiation_wh_m2,
                global_tilted_irradiation_wh_m2,
                module_temperature_dc,
                photovoltaic_energy_output_wh,
                source,
                request_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            --returning doesn't return in sqlalchemy when more than one tuple is given
            RETURNING time, plant;
        '''

        result = db_con.execute(query, readings)
        return result.rowcount

    @staticmethod
    def download_readings(from_date=None, to_date=None, processing_keys=None):

        from maintenance.db_manager import DBManager
        from conf import envinfo

        solargis_conf = envinfo.SOLARGIS

        api = ApiSolargis(**solargis_conf)
        api.create_xsd_schema()

        database_info = envinfo.DB_CONF
        with DBManager(**database_info) as dbmanager:
            with dbmanager.db_con.begin():
                api.create_table(dbmanager.db_con)

                readings = api.get_current_solargis_irradiance_readings(from_date=from_date, to_date=to_date, processing_keys=processing_keys)

                if processing_keys == 'GHI GTI TMOD PVOUT':
                    api.save_to_db(dbmanager.db_con, readings)
                elif processing_keys == 'GHI GTI TMOD':
                    readings = [(t, ghi, gti, tmod, None, source, request_time) for t, ghi, gti, tmod, source, request_time in readings]
                    api.save_to_db(dbmanager.db_con, readings)
                else:
                    logger.info("database expects GHI GTI TMOD PVOUT, we're not saving. Just showing you the result.")
                    logger.info(readings)


    @staticmethod
    def daily_download_readings():

        from maintenance.db_manager import DBManager
        from conf import envinfo

        solargis_conf = envinfo.SOLARGIS

        api = ApiSolargis(**solargis_conf)
        api.create_xsd_schema()

        database_info = envinfo.DB_CONF
        with DBManager(**database_info) as dbmanager:
            with dbmanager.db_con.begin():
                api.create_table(dbmanager.db_con)
                readings = api.get_current_solargis_readings_standarized()
                api.save_to_db(dbmanager.db_con, readings)


if __name__ == '__main__':
    import sys
    try:
        if len(sys.argv < 3):
            logger.error("Missing paramaters. expected from_date to_date [processing_keys]")
        else:
            from_date = datetime.strptime(sys.argv[1], '%Y-%m-%d %H:%M:%S').replace(tzinfo=datetime.timezone.utc)
            to_date = datetime.strptime(sys.argv[2], '%Y-%m-%d %H:%M:%S').replace(tzinfo=datetime.timezone.utc)
            processing_keys = ' '.join(sys.argv[3:]) if len(sys.argv) > 3 else None
            ApiSolargis.download_readings(from_date, to_date, processing_keys)
    except Exception as err:
        logger.error("[ERROR] %s" % err)
        raise
