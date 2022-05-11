from operator import invert
import requests

import datetime

import xmlschema

from typing import NamedTuple, Dict

from conf.log import logger

from plantmonitor.utils import rfc3336todt

# class Module(NamedTuple):
#     degradation_mpercent: int
#     degradation_first_year_mpercent: int
#     surface_reflectance_dc: int
#     nominal_operating_cell_temp_dc: int

# class Inverter(NamedTuple):
#     efficiency_mpercent: int
#     limitation_ac_power_w: int

# class Losses(NamedTuple):
#     dc_losses_mpercent: Dict[str,int]
#     ac_losses_mpercent: Dict[str,int]

class Site(NamedTuple):
    id: int
    name: str
    peak_power_w: int
    latitude: float
    longitude: float
    installation_type: str

class ApiSolargis:

    def __init__(self, api_base_url, api_key):

        self.api_base_url = api_base_url
        self.api_request_url = "{}/{}".format(api_base_url, "datadelivery/request")
        self.api_key = api_key
        self.api_name = 'solargis'

        self.api_key_params = {'key': api_key}

        # TODO set from database wither via Pony, sqlalchemy or else if we stick with a satellite api
        # {plant_id: (lat, long)}

        self.sites = {
            3: Site(
                id=3,
                name='Fontivsolar',
                latitude=40.932389,
                longitude=-4.968694,
                peak_power_w=990,
                installation_type='FREE_STANDING',
            ),
            9: Site(
                id=9,
                name='Picanya',
                latitude=39.440722,
                longitude=-0.428722,
                peak_power_w=335,
                installation_type='FREE_STANDING',
            ),
            22: Site(
                id=22,
                name='Llanillos',
                latitude=37.504330,
                longitude=-3.236476,
                peak_power_w=3820,
                installation_type='FREE_STANDING',
            ),
        }

    @staticmethod
    def get_system_xml(site: Site):
        # TODO OO this once we have a few examples and an idea of the topology

        module_xml = '''
            <pv:module type="CSI">
                <pv:degradation>0.3</pv:degradation>
                <pv:degradationFirstYear>0.8</pv:degradationFirstYear>
                <pv:nominalOperatingCellTemp>45</pv:nominalOperatingCellTemp>
                <pv:PmaxCoeff>-0.38</pv:PmaxCoeff>
            </pv:module>
        '''
        inverter_xml = '''
            <pv:inverter>
                <pv:efficiency xsi:type="pv:EfficiencyConstant" percent="97.5"/>
                <!--<pv:efficiency xsi:type="pv:EfficiencyCurve" dataPairs="0:20 50:60 100:80 150:90 233:97.5 350:97 466:96.5 583:96 700:95.5 750:93.33 800:87.5 850:82.35 900:77.8 950:73.7"/>-->
                <pv:limitationACPower>900</pv:limitationACPower>
            </pv:inverter>
        '''

        losses_xml = '''
            <pv:losses>
                <pv:acLosses cables="0.1" transformer="0.9"/>
                <pv:dcLosses cables="0.2" mismatch="0.3" snowPollution="3.0"/>
                <!-- <pv:dcLosses cables="0.2" mismatch="0.3" monthlySnowPollution="5 5.2 3 1 1 1 1 1 1 1 2 4"/> -->
            </pv:losses>
        '''

        topology_xml = '''
           <pv:topology xsi:type="pv:TopologySimple" relativeSpacing="2.4" type="UNPROPORTIONAL2"/>
           <!-- <pv:topology xsi:type="pv:TopologyColumn" relativeSpacing="2.5" type="UNPROPORTIONAL2"/> -->
        '''

        system = f'''
        <pv:system installedPower="{site.peak_power_w}" installationType="{site.installation_type}" dateStartup="2022-01-03" selfShading="true">
            {module_xml}
            {inverter_xml}
            {losses_xml}
            {topology_xml}
        </pv:system>
        '''

        return system

    def create_xsd_schema(self):
        self.schema = xmlschema.XMLSchema('data/ws-data.xsd')

    def get_arbitrary_payload(self, xml_request_content):
        headers = {'Content-Type' : 'application/xml'}

        try:
            response = requests.post(
                self.api_request_url,
                params=self.api_key_params,
                headers=headers,
                data=xml_request_content.encode('utf8'),
                timeout=30
            )
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error("Request exception {}".format(e))
            return response.status_code, None

        if response.status_code != 200:
            logger.error("Request error {}".format(response.text))

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

    def get_current_solargis_irradiance_readings_site(self, site: Site, from_date, to_date, processing_keys):

        from_date_str = datetime.datetime.strftime(from_date, '%Y-%m-%d')
        to_date_str = datetime.datetime.strftime(to_date, '%Y-%m-%d')

        system = ApiSolargis.get_system_xml(site)

        xml_request_content = f'''
            <ws:dataDeliveryRequest dateFrom="{from_date_str}" dateTo="{to_date_str}"
            xmlns="http://geomodel.eu/schema/data/request"
            xmlns:ws="http://geomodel.eu/schema/ws/data"
            xmlns:geo="http://geomodel.eu/schema/common/geo"
            xmlns:pv="http://geomodel.eu/schema/common/pv"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <site id="{site.name}" lat="{site.latitude}" lng="{site.longitude}">
                {system}
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
        from_date = from_date or datetime.date.today() - datetime.timedelta(days=1)
        to_date = to_date or datetime.date.today() - datetime.timedelta(days=1)

        # TODO add a requests session here to reuse TCP connection
        all_readings = []
        for plant_id, site in self.sites.items():
            plant_name = site.name
            status, readings = self.get_current_solargis_irradiance_readings_site(site, from_date, to_date, processing_keys)
            if status != 200:
                logger.error(f"Error reading plant {plant_id} {plant_name} {status}")
            else:
                all_readings = [
                    (t, plant_id, *values)
                    for t, *values in readings
                ]

        return all_readings

    def get_current_solargis_readings_standarized(self, from_date=None, to_date=None):

        processing_keys = 'GHI GTI TMOD PVOUT'
        from_date = from_date or datetime.date.today() - datetime.timedelta(days=1)
        to_date = to_date or datetime.date.today() - datetime.timedelta(days=1)

        all_readings = []
        for plant_id, site in self.sites.items():
            status, readings = self.get_current_solargis_irradiance_readings_site(site, from_date, to_date, processing_keys)
            if status != 200:
                logger.error(f"Error reading plant {plant_id} {site.name} {status}")
            else:
                all_readings = [
                    (t, plant_id, int(ghi), int(gti), int(tmod*10), int(pvout*1000), None, source, request_time)
                    for t, ghi, gti, tmod, pvout, source, request_time in readings
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

                if processing_keys is None or processing_keys == 'GHI GTI TMOD PVOUT':
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
        if len(sys.argv) < 3:
            logger.error("Missing paramaters. expected from_date to_date [processing_keys]")
        else:
            from_date = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
            to_date = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
            processing_keys = ' '.join(sys.argv[3:]) if len(sys.argv) > 3 else None
            ApiSolargis.download_readings(from_date, to_date, processing_keys)
    except Exception as err:
        logger.error("[ERROR] %s" % err)
        raise
