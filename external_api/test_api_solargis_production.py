import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.devel')
import unittest

from external_api.api_solargis import ApiSolargis, Site, PVSystem

from meteologica.utils import todtaware

# run with pytest external_api/test_api_solargis_production.py
# to ensure this script sets the PLANTMONITOR_MODULE_SETTINGS to devel
# before any other test module
@unittest.skipIf(True, "we're querying an external api, let's disable tests by default")
class ApiSolargis_ProductionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        from conf import envinfo
        solargis_conf = envinfo.SOLARGIS

        cls.api = ApiSolargis(**solargis_conf)
        cls.api.create_xsd_schema()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def demo_site(self, id=None):
        demo_system = PVSystem(
                geometry_type = 'GeometryFixedOneAngle',
                geometry_azimuth = 150,
                geometry_tilt = 5,
                geometry_backTracking = None,
                geometry_rotationLimitEast = None,
                geometry_rotationLimitWest = None,
                system_installedPower = 335,
                system_installationType = 'ROOF_MOUNTED',
                system_dateStartup = '2012-09-01',
                system_selfShading = 'true',
                module_type = 'CSI',
                degradation_content = 0.71,
                degradationFirstYear_content = 2.00,
                PmaxCoeff_content = -0.41,
                efficiency_type = 'EfficiencyConstant',
                efficiency_content = 94.80,
                limitationACPower_content = 290,
                dcLosses_snowPollution = 8.00,
                dcLosses_cables = 2.00,
                dcLosses_mismatch = 1.00,
                acLosses_transformer = 0.50,
                acLosses_cables = 0.50,
                topology_xsi_type = 'TopologyRow',
                topology_relativeSpacing = 1.2,
                topology_type = 'UNPROPORTIONAL1',
        )

        return Site(
            id=id or 1, name="Picanya", latitude=39.440722, longitude=-0.428722,
            peak_power_w=1000, installation_type='FREE_STANDING', pvsystem=demo_system
        )

    def demo_site_high_loss(self, id=None):
        demo_system = PVSystem(
                geometry_type='GeometryFixedOneAngle',
                geometry_azimuth=180,
                geometry_tilt=30,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=2160,
                system_installationType='FREE_STANDING',
                system_dateStartup='2016-01-01',
                system_selfShading='true',
                module_type='CSI',
                degradation_content=0.63,
                degradationFirstYear_content=2.50,
                PmaxCoeff_content=-0.442,
                efficiency_type='EfficiencyConstant',
                efficiency_content=97.50,
                limitationACPower_content=1890,
                dcLosses_snowPollution=3.00,
                dcLosses_cables=2.0,
                dcLosses_mismatch=1.5,
                acLosses_transformer=0.50,
                acLosses_cables=0.50,
                topology_xsi_type='TopologyRow',
                topology_relativeSpacing=2,
                topology_type='UNPROPORTIONAL1',
        )

        return Site(
            id=id or 1,
            name='Alcolea',
            latitude=37.628645,
            longitude=-5.680761,
            peak_power_w=2160,
            installation_type='FREE_STANDING',
            pvsystem=demo_system
        )

    def demo_site_low_loss(self, id=None):
        demo_system = PVSystem(
                geometry_type='GeometryFixedOneAngle',
                geometry_azimuth=180,
                geometry_tilt=30,
                geometry_backTracking=None,
                geometry_rotationLimitEast=None,
                geometry_rotationLimitWest=None,
                system_installedPower=2160,
                system_installationType='FREE_STANDING',
                system_dateStartup='2016-01-01',
                system_selfShading='true',
                module_type='CSI',
                degradation_content=0.63,
                degradationFirstYear_content=2.50,
                PmaxCoeff_content=-0.442,
                efficiency_type='EfficiencyConstant',
                efficiency_content=97.50,
                limitationACPower_content=1890,
                dcLosses_snowPollution=2.00,
                dcLosses_cables=1.5,
                dcLosses_mismatch=1.0,
                acLosses_transformer=0.50,
                acLosses_cables=0.50,
                topology_xsi_type='TopologyRow',
                topology_relativeSpacing=2,
                topology_type='UNPROPORTIONAL1',
        )

        return Site(
            id=id or 1,
            name='Alcolea',
            latitude=37.628645,
            longitude=-5.680761,
            peak_power_w=2160,
            installation_type='FREE_STANDING',
            pvsystem=demo_system
        )


    def test__is_production(self):
        self.assertNotEqual(self.api.api_key, 'demo')

    def __test__get_current_solargis_irradiance_readings__production_credentials(self):
        self.maxDiff = None

        from_date = todtaware('2025-01-01 13:00:00')
        to_date = todtaware('2025-01-01 15:00:00')

        self.api.sites = {
            3: self.demo_site()
        }

        readings = self.api.get_current_solargis_irradiance_readings(from_date, to_date)

        self.assertEqual(len(readings), 24)
        # self.assertListEqual(list(readings['data'][0].keys()), list(expected['data'][0].keys()))


    def test__get_solargis_readings__high_vs_low_loss(self):
        self.maxDiff = None

        processing_keys = 'GHI GTI TMOD PVOUT'
        from_date = todtaware('2025-01-01 13:00:00')
        to_date = todtaware('2025-01-01 15:00:00')

        self.api.sites = {
            1: self.demo_site_high_loss(1),
            2: self.demo_site_low_loss(2)
        }

        status, readings_high = self.api.get_current_solargis_irradiance_readings_site(self.api.sites[1], from_date, to_date, processing_keys)
        status, readings_low = self.api.get_current_solargis_irradiance_readings_site(self.api.sites[2], from_date, to_date, processing_keys)

        # todo select energy
        self.assertNotEqual(readings_high, readings_low)
        # self.assertListEqual(list(readings['data'][0].keys()), list(expected['data'][0].keys()))
