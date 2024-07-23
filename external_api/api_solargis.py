from operator import invert
import requests

import datetime

import xmlschema

from typing import NamedTuple, Dict, Optional

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


class PVSystem(NamedTuple):
    geometry_type: str
    geometry_azimuth: int
    geometry_tilt: int
    geometry_backTracking: bool
    geometry_rotationLimitEast: int
    geometry_rotationLimitWest: int
    system_installedPower: int  # TO BE DELETED
    system_installationType: str  # TO BE DELETED
    system_dateStartup: str
    system_selfShading: bool
    module_type: str
    degradation_content: float
    degradationFirstYear_content: float
    PmaxCoeff_content: float
    efficiency_type: str
    efficiency_content: float
    limitationACPower_content: int
    dcLosses_snowPollution: float
    dcLosses_cables: float
    dcLosses_mismatch: float
    acLosses_transformer: float
    acLosses_cables: float
    topology_xsi_type: str
    topology_relativeSpacing: float
    topology_type: str
    tiltLimitMin: Optional[int] = None
    tiltLimitMax: Optional[int] = None


class Site(NamedTuple):
    id: int
    name: str
    peak_power_w: int
    latitude: float
    longitude: float
    installation_type: str
    pvsystem: PVSystem


class ApiSolargis:

    def __init__(self, api_base_url, api_key):

        self.api_base_url = api_base_url
        self.api_request_url = "{}/{}".format(api_base_url, "datadelivery/request")
        self.api_key = api_key
        self.api_name = "solargis"

        self.api_key_params = {"key": api_key}

        # TODO set from database wither via Pony, sqlalchemy or else if we stick with a satellite api
        # {plant_id: (lat, long)}

        # Data fetched from here:
        # https://docs.google.com/spreadsheets/d/1J2G6IuqIxIXT4ETG3vxVpryszs9Or_ss8NtMnGCJ6yw/edit#gid=0
        pvsystems = {
            9: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=170,
                geometry_tilt=2,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=333.73,
                system_installationType="ROOF_MOUNTED",
                system_dateStartup="2012-09-01",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.71,
                degradationFirstYear_content=2.00,
                PmaxCoeff_content=-0.41,
                efficiency_type="EfficiencyConstant",
                efficiency_content=94.80,
                limitationACPower_content=290,
                dcLosses_snowPollution=2.00,
                dcLosses_cables=0.75,
                dcLosses_mismatch=1.00,
                acLosses_transformer=0.50,
                acLosses_cables=0.50,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=1.8,
                topology_type="UNPROPORTIONAL1",
            ),
            3: PVSystem(
                geometry_type="GeometryOneAxisHorizontalNS",
                geometry_azimuth=225,
                geometry_tilt=0,
                geometry_backTracking="true",
                geometry_rotationLimitEast=-45,
                geometry_rotationLimitWest=45,
                system_installedPower=988,
                system_installationType="FREE_STANDING",
                system_dateStartup="2018-05-01",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.68,
                degradationFirstYear_content=2.00,
                PmaxCoeff_content=-0.41,
                efficiency_type="EfficiencyConstant",
                efficiency_content=98.10,
                limitationACPower_content=800,
                dcLosses_snowPollution=2.00,
                dcLosses_cables=1.00,
                dcLosses_mismatch=1,
                acLosses_transformer=0.50,
                acLosses_cables=0.75,
                topology_xsi_type="TopologyColumn",
                topology_relativeSpacing=2,
                topology_type="UNPROPORTIONAL1",
            ),
            22: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=177,
                geometry_tilt=31,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=3860,
                system_installationType="FREE_STANDING",
                system_dateStartup="2021-03-01",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.59,
                degradationFirstYear_content=1.50,
                PmaxCoeff_content=-0.4,
                efficiency_type="EfficiencyConstant",
                efficiency_content=98.50,
                limitationACPower_content=3200,
                dcLosses_snowPollution=2.00,
                dcLosses_cables=1.0,
                dcLosses_mismatch=1,
                acLosses_transformer=0.50,
                acLosses_cables=0.50,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=2.2,
                topology_type="UNPROPORTIONAL1",
            ),
            1: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=180,
                geometry_tilt=31,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=2160,
                system_installationType="FREE_STANDING",
                system_dateStartup="2016-01-01",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.63,
                degradationFirstYear_content=2.00,
                PmaxCoeff_content=-0.442,
                efficiency_type="EfficiencyConstant",
                efficiency_content=97.50,
                limitationACPower_content=1890,
                dcLosses_snowPollution=2.00,
                dcLosses_cables=1.5,
                dcLosses_mismatch=1.0,
                acLosses_transformer=0.50,
                acLosses_cables=0.50,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=2.3,
                topology_type="UNPROPORTIONAL1",
            ),
            7: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=182,
                geometry_tilt=28,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=2376,
                system_installationType="FREE_STANDING",
                system_dateStartup="2018-01-01",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.63,
                degradationFirstYear_content=1.50,
                PmaxCoeff_content=-0.442,
                efficiency_type="EfficiencyConstant",
                efficiency_content=97.50,
                limitationACPower_content=2000,
                dcLosses_snowPollution=2.00,
                dcLosses_cables=1.0,
                dcLosses_mismatch=0.5,
                acLosses_transformer=0.50,
                acLosses_cables=0.25,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=2,
                topology_type="UNPROPORTIONAL1",
            ),
            6: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=184,
                geometry_tilt=26,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=1650,
                system_installationType="FREE_STANDING",
                system_dateStartup="2019-10-30",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.57,
                degradationFirstYear_content=1.50,
                PmaxCoeff_content=-0.41,
                efficiency_type="EfficiencyConstant",
                efficiency_content=97.50,
                limitationACPower_content=1400,
                dcLosses_snowPollution=2.00,
                dcLosses_cables=1.0,
                dcLosses_mismatch=1.0,
                acLosses_transformer=0.50,
                acLosses_cables=0.50,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=2.5,
                topology_type="UNPROPORTIONAL1",
            ),
            13: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=165,
                geometry_tilt=28,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=841,
                system_installationType="FREE_STANDING",
                system_dateStartup="2020-10-14",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.5,
                degradationFirstYear_content=1.50,
                PmaxCoeff_content=-0.35,
                efficiency_type="EfficiencyConstant",
                efficiency_content=98.3,
                limitationACPower_content=720,
                dcLosses_snowPollution=2.00,
                dcLosses_cables=1.0,
                dcLosses_mismatch=0.25,
                acLosses_transformer=0.50,
                acLosses_cables=0.25,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=2.5,
                topology_type="UNPROPORTIONAL1",
            ),
            5: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=200,
                geometry_tilt=30,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=19.2,
                system_installationType="FREE_STANDING",
                system_dateStartup="2015-07-01",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.7,
                degradationFirstYear_content=2.0,
                PmaxCoeff_content=-0.4,
                efficiency_type="EfficiencyConstant",
                efficiency_content=97.9,
                limitationACPower_content=18,
                dcLosses_snowPollution=2.00,
                dcLosses_cables=1.00,
                dcLosses_mismatch=1.00,
                acLosses_transformer=0.0,
                acLosses_cables=0.50,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=2.3,
                topology_type="UNPROPORTIONAL1",
            ),
            2: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=225,
                geometry_tilt=10,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=22.4,
                system_installationType="FREE_STANDING",
                system_dateStartup="2012-08-01",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.7,
                degradationFirstYear_content=2.5,
                PmaxCoeff_content=-0.4,
                efficiency_type="EfficiencyConstant",
                efficiency_content=97.9,
                limitationACPower_content=20,
                dcLosses_snowPollution=2.00,
                dcLosses_cables=1.00,
                dcLosses_mismatch=0.50,
                acLosses_transformer=0.0,
                acLosses_cables=0.75,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=1.5,
                topology_type="UNPROPORTIONAL1",
            ),
            4: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=210,
                geometry_tilt=17,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=22.4,
                system_installationType="FREE_STANDING",
                system_dateStartup="2012-08-01",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.7,
                degradationFirstYear_content=2.5,
                PmaxCoeff_content=-0.4,
                efficiency_type="EfficiencyConstant",
                efficiency_content=98.3,
                limitationACPower_content=20,
                dcLosses_snowPollution=2.00,
                dcLosses_cables=1.00,
                dcLosses_mismatch=0.50,
                acLosses_transformer=0.0,
                acLosses_cables=0.50,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=1.8,
                topology_type="UNPROPORTIONAL3",
            ),
            14: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=190,
                geometry_tilt=12,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=95.01,
                system_installationType="FREE_STANDING",
                system_dateStartup="2013-03-04",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.7,
                degradationFirstYear_content=2.5,
                PmaxCoeff_content=-0.4,
                efficiency_type="EfficiencyConstant",
                efficiency_content=98.1,
                limitationACPower_content=90,
                dcLosses_snowPollution=2,
                dcLosses_cables=1.00,
                dcLosses_mismatch=0.25,
                acLosses_transformer=0.0,
                acLosses_cables=0.5,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=1.9,
                topology_type="UNPROPORTIONAL2",
            ),
            15: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=153,
                geometry_tilt=5,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=109.44,
                system_installationType="ROOF_MOUNTED",
                system_dateStartup="2013-03-04",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.7,
                degradationFirstYear_content=2.5,
                PmaxCoeff_content=-0.4,
                efficiency_type="EfficiencyConstant",
                efficiency_content=98.1,
                limitationACPower_content=100,
                dcLosses_snowPollution=2,
                dcLosses_cables=2.50,
                dcLosses_mismatch=1.00,
                acLosses_transformer=0.0,
                acLosses_cables=0.25,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=2,
                topology_type="UNPROPORTIONAL1",
            ),
            16: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=215,
                geometry_tilt=25,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=103,
                system_installationType="FREE_STANDING",
                system_dateStartup="2012-03-07",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.7,
                degradationFirstYear_content=1.5,
                PmaxCoeff_content=-0.4,
                efficiency_type="EfficiencyConstant",
                efficiency_content=96.6,
                limitationACPower_content=99,
                dcLosses_snowPollution=2,
                dcLosses_cables=1.50,
                dcLosses_mismatch=0.00,
                acLosses_transformer=0.0,
                acLosses_cables=0.5,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=1.8,
                topology_type="UNPROPORTIONAL2",
            ),
            17: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=190,
                geometry_tilt=15,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=99,
                system_installationType="FREE_STANDING",
                system_dateStartup="2013-03-15",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.6,
                degradationFirstYear_content=1.0,
                PmaxCoeff_content=-0.38,
                efficiency_type="EfficiencyConstant",
                efficiency_content=98.1,
                limitationACPower_content=90,
                dcLosses_snowPollution=1,
                dcLosses_cables=0.50,
                dcLosses_mismatch=0.0,
                acLosses_transformer=0.0,
                acLosses_cables=0.5,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=2.0,
                topology_type="UNPROPORTIONAL1",
            ),
            40: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=182,
                geometry_tilt=28,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=3998.0,
                system_installationType="FREE_STANDING",
                system_dateStartup="2021-10-01",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.50,
                degradationFirstYear_content=0.0,
                PmaxCoeff_content=-0.45,
                efficiency_type="EfficiencyConstant",
                efficiency_content=98.6,
                limitationACPower_content=3800,
                dcLosses_snowPollution=0,
                dcLosses_cables=1.00,
                dcLosses_mismatch=0.50,
                acLosses_transformer=1.0,
                acLosses_cables=0.5,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=2.2,
                topology_type="UNPROPORTIONAL1",
            ),
            41: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=160,
                geometry_tilt=23,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=113.16,
                system_installationType="ROOF_MOUNTED",
                system_dateStartup="2023-02-01",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.55,
                degradationFirstYear_content=2.50,
                PmaxCoeff_content=-0.35,
                efficiency_type="EfficiencyConstant",
                efficiency_content=98.0,
                limitationACPower_content=100,
                dcLosses_snowPollution=2.00,
                dcLosses_cables=1.5,
                dcLosses_mismatch=1.0,
                acLosses_transformer=0.0,
                acLosses_cables=1.0,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=2.2,
                topology_type="UNPROPORTIONAL1",
            ),
            # Hedensted
            19: PVSystem(
                geometry_type="GeometryFixedOneAngle",
                geometry_azimuth=180,
                geometry_tilt=30,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=840.00,
                system_installationType="FREE_STANDING",
                system_dateStartup="2012-01-01",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.60,
                degradationFirstYear_content=2.50,
                PmaxCoeff_content=-0.43,
                efficiency_type="EfficiencyConstant",
                efficiency_content=98.0,
                limitationACPower_content=800.00,
                dcLosses_snowPollution=2.0,
                dcLosses_cables=1.50,
                dcLosses_mismatch=0.50,
                acLosses_transformer=1.00,
                acLosses_cables=1.00,
                topology_xsi_type="TopologyRow",
                topology_relativeSpacing=2.2,
                topology_type="UNPROPORTIONAL1",
            ),
            # Svendborg
            20: PVSystem(
                geometry_type="GeometryTwoAxisAstronomical",
                geometry_azimuth=None,
                geometry_tilt=None,
                geometry_backTracking="true",
                geometry_rotationLimitEast=-45,
                geometry_rotationLimitWest=45,
                system_installedPower=622.08,
                system_installationType="FREE_STANDING",
                system_dateStartup="2012-01-01",
                system_selfShading="true",
                module_type="CSI",
                degradation_content=0.60,
                degradationFirstYear_content=2.50,
                PmaxCoeff_content=-0.43,
                efficiency_type="EfficiencyConstant",
                efficiency_content=98.0,
                limitationACPower_content=594.00,
                dcLosses_snowPollution=2.0,
                dcLosses_cables=1.50,
                dcLosses_mismatch=0.50,
                acLosses_transformer=1.00,
                acLosses_cables=1.00,
                topology_xsi_type="TopologyColumn",
                topology_relativeSpacing=4.0,
                topology_type="UNPROPORTIONAL1",
            ),
        }

        self.sites = {
            9: Site(
                id=9,
                name="Picanya",
                latitude=39.440722,
                longitude=-0.428722,
                peak_power_w=333.73,
                installation_type="ROOF_MOUNTED",
                pvsystem=pvsystems[9],
            ),
            3: Site(
                id=3,
                name="Fontivsolar",
                latitude=40.932389,
                longitude=-4.968694,
                peak_power_w=988,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[3],
            ),
            22: Site(
                id=22,
                name="Llanillos",
                latitude=37.504330,
                longitude=-3.236476,
                peak_power_w=3820,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[22],
            ),
            1: Site(
                id=1,
                name="Alcolea",
                latitude=37.628645,
                longitude=-5.680761,
                peak_power_w=2160,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[1],
            ),
            7: Site(
                id=7,
                name="Matallana",
                latitude=37.653271,
                longitude=-5.560263,
                peak_power_w=2376,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[7],
            ),
            6: Site(
                id=6,
                name="Florida",
                latitude=37.634456,
                longitude=-5.587343,
                peak_power_w=1650,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[6],
            ),
            13: Site(
                id=13,
                name="Terborg",
                latitude=37.236035,
                longitude=-2.296076,
                peak_power_w=841,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[13],
            ),
            5: Site(
                id=5,
                name="Riudarenes_ZE",
                latitude=41.8157153,
                longitude=2.7190323,
                peak_power_w=19.2,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[5],
            ),
            2: Site(
                id=2,
                name="Riudarenes_SM",
                latitude=41.8157153,
                longitude=2.7190323,
                peak_power_w=22.4,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[2],
            ),
            4: Site(
                id=4,
                name="Riudarenes_BR",
                latitude=41.8157153,
                longitude=2.7190323,
                peak_power_w=22.4,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[4],
            ),
            14: Site(
                id=14,
                name="Manlleu_Piscina",
                latitude=42.000377,
                longitude=2.2848705,
                peak_power_w=95.01,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[14],
            ),
            15: Site(
                id=15,
                name="Manlleu_Pavello",
                latitude=42.000377,
                longitude=2.2848705,
                peak_power_w=109.44,
                installation_type="ROOF_MOUNTED",
                pvsystem=pvsystems[15],
            ),
            16: Site(
                id=16,
                name="Lleida",
                latitude=41.6139742,
                longitude=0.67029188,
                peak_power_w=103,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[16],
            ),
            17: Site(
                id=17,
                name="Torrefarrera",
                latitude=41.6692891,
                longitude=0.61112424,
                peak_power_w=99,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[17],
            ),
            40: Site(
                id=40,
                name="Asomada",
                latitude=37.656603,
                longitude=-0.940483,
                peak_power_w=3998,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[40],
            ),
            41: Site(
                id=41,
                name="GURB001_Mataro",
                latitude=41.5449,
                longitude=2.4498,
                peak_power_w=113.16,
                installation_type="ROOF_MOUNTED",
                pvsystem=pvsystems[41],
            ),
            19: Site(
                id=19,
                name="Hedensted",
                latitude=41.546704,
                longitude=0.80284,
                peak_power_w=840.00,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[19],
            ),
            20: Site(
                id=20,
                name="Svendborg",
                latitude=41.368032,
                longitude=0.656909,
                peak_power_w=622.08,
                installation_type="FREE_STANDING",
                pvsystem=pvsystems[20],
            ),
        }

    @staticmethod
    def get_system_xml(site: Site):
        # TODO OO this once we have a few examples and an idea of the topology

        # extras_xml = f'''
        #     <geo:terrain elevation="120" azimuth="180" tilt="5"/>
        #     <geo:horizon>0:3.6 123:5.6 359:6</geo:horizon>
        # '''

        # TODO to be implemented as necessary
        # <!-- <pv:geometry xsi:type="pv:GeometryOneAxisInclinedNS" axisTilt="30" rotationLimitEast="-90" rotationLimitWest="90" backTracking="true" azimuth="180"/> -->
        # <!-- <pv:geometry xsi:type="pv:GeometryOneAxisVertical" tilt="25" rotationLimitEast="-180" rotationLimitWest="180" backTracking="true"/> -->
        # <!-- <pv:geometry xsi:type="pv:GeometryTwoAxisAstronomical" rotationLimitEast="-180" rotationLimitWest="180" tiltLimitMin="10" tiltLimitMax="60" backTracking="true"/> -->

        if site.pvsystem.geometry_type == "GeometryOneAxisHorizontalNS":
            geo_xml = f"""
                <pv:geometry xsi:type="pv:{site.pvsystem.geometry_type}" rotationLimitEast="{site.pvsystem.geometry_rotationLimitEast}" rotationLimitWest="{site.pvsystem.geometry_rotationLimitWest}" backTracking="{site.pvsystem.geometry_backTracking}" azimuth="{site.pvsystem.geometry_azimuth}"/>
            """
        elif site.pvsystem.geometry_type == "GeometryTwoAxisAstronomical":
            geo_xml = f"""
                <pv:geometry xsi:type="pv:{site.pvsystem.geometry_type}" rotationLimitEast="{site.pvsystem.geometry_rotationLimitEast}" rotationLimitWest="{site.pvsystem.geometry_rotationLimitWest}" tiltLimitMin="{site.pvsystem.tiltLimitMin or 0}" tiltLimitMax="{site.pvsystem.tiltLimitMin or 90}" backTracking="{site.pvsystem.geometry_backTracking}"/>
            """
        elif site.pvsystem.geometry_type == "GeometryFixedOneAngle":
            geo_xml = f"""
                <pv:geometry xsi:type="pv:{site.pvsystem.geometry_type}" azimuth="{site.pvsystem.geometry_azimuth}" tilt="{site.pvsystem.geometry_tilt}"/>
            """
        else:
            raise ValueError(
                f"Unknown or not implemented geometry type {site.pvsystem.geometry_type}"
            )

        module_xml = f"""
            <pv:module type="{site.pvsystem.module_type}">
                <pv:degradation>{site.pvsystem.degradation_content}</pv:degradation>
                <pv:degradationFirstYear>{site.pvsystem.degradationFirstYear_content}</pv:degradationFirstYear>
                <pv:PmaxCoeff>{site.pvsystem.PmaxCoeff_content}</pv:PmaxCoeff>
            </pv:module>
        """

        inverter_xml = f"""
            <pv:inverter>
                <pv:efficiency xsi:type="pv:{site.pvsystem.efficiency_type}" percent="{site.pvsystem.efficiency_content}"/>
                <pv:limitationACPower>{site.pvsystem.limitationACPower_content}</pv:limitationACPower>
            </pv:inverter>
        """

        losses_xml = f"""
            <pv:losses>
                <pv:acLosses cables="{site.pvsystem.acLosses_cables}" transformer="{site.pvsystem.acLosses_transformer}"/>
                <pv:dcLosses cables="{site.pvsystem.dcLosses_cables}" mismatch="{site.pvsystem.dcLosses_mismatch}" snowPollution="{site.pvsystem.dcLosses_snowPollution}"/>
            </pv:losses>
        """

        topology_xml = f"""
           <pv:topology xsi:type="pv:{site.pvsystem.topology_xsi_type}" relativeSpacing="{site.pvsystem.topology_relativeSpacing}" type="{site.pvsystem.topology_type}"/>
        """

        system = f"""
        {geo_xml}
        <pv:system installedPower="{site.peak_power_w}" installationType="{site.installation_type}" dateStartup="{site.pvsystem.system_dateStartup}" selfShading="{site.pvsystem.system_selfShading}">
            {module_xml}
            {inverter_xml}
            {losses_xml}
            {topology_xml}
        </pv:system>
        """

        return system

    def create_xsd_schema(self):
        self.schema = xmlschema.XMLSchema("data/ws-data.xsd")

    def get_arbitrary_payload(self, xml_request_content):
        headers = {"Content-Type": "application/xml"}

        try:
            response = requests.post(
                self.api_request_url,
                params=self.api_key_params,
                headers=headers,
                data=xml_request_content.encode("utf8"),
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error("Request exception {} msg: {}".format(e, response.text))
            return response.status_code, None

        if response.status_code != 200:
            logger.error("Request error {}".format(response.text))

        text_response = response.text
        return response.status_code, text_response

    def text_response_to_readings(self, text_response, request_time=None):

        request_time = request_time or datetime.datetime.now(datetime.timezone.utc)

        response_dict = self.schema.to_dict(text_response)
        readings_dirty = response_dict["site"][0]["row"]
        readings = [
            (rfc3336todt(v["@dateTime"]), *v["@values"], self.api_name, request_time)
            for v in readings_dirty
        ]
        return readings

    def get_current_solargis_irradiance_readings_site(
        self, site: Site, from_date, to_date, processing_keys
    ):

        from_date_str = datetime.datetime.strftime(from_date, "%Y-%m-%d")
        to_date_str = datetime.datetime.strftime(to_date, "%Y-%m-%d")

        system = ApiSolargis.get_system_xml(site)

        xml_request_content = f"""
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
        """

        logger.debug(xml_request_content)

        status, text_response = self.get_arbitrary_payload(xml_request_content)

        if status == 200:
            readings = self.text_response_to_readings(text_response)
        else:
            readings = None
            logger.error(
                f"Error downloading readings from solargis {status}: {text_response}"
            )

        return status, readings

    def get_current_solargis_irradiance_readings(
        self, from_date=None, to_date=None, processing_keys=None
    ):

        processing_keys = processing_keys or "GHI GTI TMOD PVOUT"
        from_date = from_date or datetime.date.today() - datetime.timedelta(days=1)
        to_date = to_date or datetime.date.today() - datetime.timedelta(days=1)

        # TODO add a requests session here to reuse TCP connection
        all_readings = []
        for plant_id, site in self.sites.items():
            plant_name = site.name
            status, readings = self.get_current_solargis_irradiance_readings_site(
                site, from_date, to_date, processing_keys
            )
            if status != 200:
                logger.error(f"Error reading plant {plant_id} {plant_name} {status}")
            else:
                all_readings = all_readings + [
                    (t, plant_id, *values) for t, *values in readings
                ]

        return all_readings

    # For hourly requests GHI GTI and PVOUT [Wh/m2] with mWh/m2 resolution TMOD [C] with one decimal resolution
    def get_current_solargis_readings_standarized(
        self, from_date=None, to_date=None, selected_site_ids=None
    ):

        processing_keys = "GHI GTI TMOD PVOUT"
        from_date = from_date or datetime.date.today() - datetime.timedelta(days=1)
        to_date = to_date or datetime.date.today() - datetime.timedelta(days=1)

        all_readings = []

        if selected_site_ids:
            sites = {
                site_id: site
                for site_id, site in self.sites.items()
                if site_id in selected_site_ids
            }
        else:
            sites = self.sites

        for plant_id, site in sites.items():
            status, readings = self.get_current_solargis_irradiance_readings_site(
                site, from_date, to_date, processing_keys
            )
            if status != 200:
                logger.error(f"Error reading plant {plant_id} {site.name} {status}")
            else:
                all_readings = all_readings + [
                    (
                        t,
                        plant_id,
                        int(ghi),
                        int(gti),
                        int(tmod * 10) if tmod != -99 else None,
                        int(pvout),
                        source,
                        request_time,
                    )
                    for t, ghi, gti, tmod, pvout, source, request_time in readings
                ]

        return all_readings

    # TODO use the db_factory
    @staticmethod
    def create_table(db_con):
        alarm_registry = """
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
        """
        db_con.execute(alarm_registry)

    def save_to_db(self, db_con, readings):

        query = """
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
        """

        result = db_con.execute(query, readings)
        return result.rowcount

    @staticmethod
    def download_readings(
        solargis_conf,
        database_info,
        from_date=None,
        to_date=None,
        processing_keys=None,
        plant_ids=None,
    ):

        from maintenance.db_manager import DBManager

        api = ApiSolargis(**solargis_conf)
        api.create_xsd_schema()

        num_rows = 0

        with DBManager(**database_info) as dbmanager:
            with dbmanager.db_con.begin():
                api.create_table(dbmanager.db_con)

                if not processing_keys or processing_keys == "GHI GTI TMOD PVOUT":
                    readings = api.get_current_solargis_readings_standarized(
                        from_date=from_date,
                        to_date=to_date,
                        selected_site_ids=plant_ids,
                    )
                    if readings:
                        num_rows = api.save_to_db(dbmanager.db_con, readings)

                else:
                    readings = api.get_current_solargis_irradiance_readings(
                        from_date=from_date,
                        to_date=to_date,
                        processing_keys=processing_keys,
                    )

                    logger.info(
                        "database expects GHI GTI TMOD PVOUT, we're not saving. Just showing you the result."
                    )
                    logger.info(readings)

        return num_rows

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


if __name__ == "__main__":
    import sys

    try:
        if len(sys.argv) < 3:
            logger.error(
                "Missing paramaters. expected from_date to_date [processing_keys]"
            )
        else:
            from conf import envinfo

            solargis_conf = envinfo.SOLARGIS
            database_info = envinfo.DB_CONF

            from_date = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d").replace(
                tzinfo=datetime.timezone.utc
            )
            to_date = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d").replace(
                tzinfo=datetime.timezone.utc
            )
            processing_keys = " ".join(sys.argv[3:]) if len(sys.argv) > 3 else None
            ApiSolargis.download_readings(
                solargis_conf, database_info, from_date, to_date, processing_keys
            )
    except Exception as err:
        logger.error("[ERROR] %s" % err)
        raise
