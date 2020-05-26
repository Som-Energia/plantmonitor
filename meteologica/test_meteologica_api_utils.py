from meteologica.meteologica_api_utils import (
    MeteologicaApi_Mock,
    MeteologicaApi,
    MeteologicaApiError,
)

from meteologica.utils import todt

#from django.conf import settings
from yamlns import namespace as ns
from pathlib import Path
from unittest.mock import patch
import unittest

import logging.config

logging.config.dictConfig({
    'version': 1,
    'formatters': {
        'verbose': {
            'format': '%(name)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        'zeep.transports': {
            'level': 'INFO',
            'propagate': True,
            'handlers': ['console'],
        },
    }
})


class MeteologicaApiMock_Test(unittest.TestCase):

    def createApi(self):
        return MeteologicaApi_Mock()

    def mainFacility(self):
        return "MyPlant"

    def otherFacility(slf):
        return "OtherPlant"

    def test_uploadProduction_singleData(self):
        facility = self.mainFacility()
        api = self.createApi()
        api.uploadProduction(facility, [
            (todt("2040-01-01 00:00:00"), 10),
        ])
        self.assertEqual(
            api.lastDateUploaded(facility),
            todt("2040-01-01 00:00:00")
        )

    def test_uploadProduction_noData(self):
        facility = self.mainFacility()
        api = self.createApi()
        self.assertEqual(
            api.lastDateUploaded(facility),
            None
        )

    def test_uploadProduction_manyData(self):
        facility = self.mainFacility()
        api = self.createApi()
        api.uploadProduction(facility, [
            (todt("2040-01-01 00:00:00"), 10),
            (todt("2040-01-02 00:00:00"), 10),
        ])
        self.assertEqual(
            api.lastDateUploaded(facility),
            todt("2040-01-02 00:00:00")
        )

    def test_uploadProduction_calledTwice(self):
        facility = self.mainFacility()
        api = self.createApi()
        api.uploadProduction(facility, [
            (todt("2040-01-02 00:00:00"), 10),
        ])
        api.uploadProduction(facility, [
            (todt("2040-01-01 00:00:00"), 10),
        ])
        self.assertEqual(
            api.lastDateUploaded(facility),
            todt("2040-01-02 00:00:00"),
        )

    def test_uploadProduction_doesNotChangeOtherFacility(self):
        facility = self.mainFacility()
        api = self.createApi()
        api.uploadProduction(facility, [
            (todt("2040-01-01 00:00:00"), 10),
        ])
        self.assertEqual(
            api.lastDateUploaded("OtherPlant"),
            None
        )

    def test_uploadProduction_otherFacility(self):
        facility = self.mainFacility()
        otherFacility = self.otherFacility()
        api = self.createApi()
        api.uploadProduction(facility, [
            (todt("2040-01-02 00:00:00"), 10),
        ])
        api.uploadProduction(otherFacility, [
            (todt("2040-01-01 00:00:00"), 10),
        ])
        self.assertEqual(
            api.lastDateUploaded(otherFacility),
            todt("2040-01-01 00:00:00")
        )

    def test_uploadProduction_wrongFacility(self):
        api = self.createApi()
        with self.assertRaises(MeteologicaApiError) as ctx:
            api.uploadProduction("WrongPlant", [
                (todt("2040-01-01 00:00:00"), 10),
            ])
        self.assertEqual(type(u'')(ctx.exception), "INVALID_FACILITY_ID")
        self.assertEqual(api.lastDateUploaded("WrongPlant"), None)

    def _test_login_wrongSessionLogin(self):
        api = self.createApi()
        login = api.login('alberto','124')
        self.assertEqual(
            login['errorCode'],
            'INVALID_USERNAME_OR_PASSWORD'
        )

    def _test_login_rightSessionLogin(self):
        api = self.createApi()
        login = api.login('alberto','1234')
        self.assertEqual(
            login['errorCode'],
            'OK'
        )

    def _test_uploadProduction_errorUpload(self):
        api = self.createApi()
        login = api.login('alberto','1234')
        facility = self.mainFacility()
        api.uploadProduction(facility, [
            (todt("2040-01-01 00:00:00"), 10),
        ])
  
    def test_dateDownloadProduction(self):
        api = self.createApi()
        facility = self.mainFacility()
        api.uploadProduction(facility, [
            (todt("2020-01-01 00:00:00"), 10),
        ])
        result = api.downloadProduction(
                    facility,
                    todt("2020-01-01 00:00:00"),
                    todt("2020-01-01 00:00:00"),
                )

        #expected [("2020-01-01 00:00:00", _)] since we don't know meteologica's algorithm
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(result[0][0], todt("2020-01-01 00:00:00"))

    def test_getFacilities(self):
        pass

    def __test_addFacility(self):
        pass

class MeteologicaApi_Test(MeteologicaApiMock_Test):

    def createApi(self, **kwds):
        configApi = self.createConfig()
        params=dict(
            wsdl=configApi['meteo_test_url'],
            username=configApi['meteo_user'],
            password=configApi['meteo_password'],
            lastDateFile='lastDateFile.yaml',
            lastDateDownloadFile='lastDateDownloadFile.yaml',
            showResponses=False,
        )
        params.update(kwds)
        return MeteologicaApi(**params)
    
    def createConfig(self):
        return ns.load('conf/configdb_test.yaml')

    def setUp(self):        
        self.cleanLastDateFile()

    def tearDown(self):
        self.cleanLastDateFile()

    def cleanLastDateFile(self):
        f = Path('lastDateFile.yaml')
        if f.exists(): f.unlink()

    def mainFacility(self):
        return "SomEnergia_Fontivsolar"

    def otherFacility(self):
        return "SomEnergia_Alcolea"

    def test_uploadProduction_lastDateUploadedIsPersistent(self):
        facility = self.mainFacility()
        api = self.createApi()
        api.uploadProduction(facility, [
            (todt("2040-01-01 00:00:00"), 10),
        ])
        api2 = self.createApi()
        self.assertEqual(
            api2.lastDateUploaded(facility),
            todt("2040-01-01 00:00:00")
        )

    def test_session_unitializedOnConstruction(self):
        api = self.createApi()
        self.assertFalse(api.session())
    
    def test_session_withinContextIsTrue(self):
        api = self.createApi()
        with api:
            self.assertTrue(api.session()) 

    def test_session_outsideContextLogout(self):
        api = self.createApi()
        with api:
            pass
        self.assertFalse(api.session()) 

    def test_session_keepSession(self):
        facility = self.mainFacility()
        api = self.createApi()
        with api:
            session = api.session()
            api.uploadProduction(facility, [
                (todt("2040-01-01 00:00:00"), 10),
            ])
            self.assertEqual(session, api.session())
    
    def test_login_rightSessionLogin(self):
        api = self.createApi()
        api.login()
        self.assertTrue(api.session())
    
    def test_login_wrongSessionLogin(self):
        api = self.createApi(username='badUser')
        with self.assertRaises(MeteologicaApiError) as ctx:
            api.login()
        self.assertEqual(type(u'')(ctx.exception),'INVALID_USERNAME_OR_PASSWORD')
        self.assertFalse(api.session())
    
    def __test_getLastApiDate(self):
        api = self.createApi()
        with api:
            api.uploadProduction(facility, [
                (todt("2040-02-01 00:00:00"), 10),
            ])
            result = api.getLastApiDate()
        self.assertEqual(result, todt("2040-02-01 00:00:00"))

    def __test_getLastApiDateForFacility(self):
        facility = self.mainFacility()
        api = self.createApi()
        with api:
            api.uploadProduction(facility, [
                (todt("2040-02-01 00:00:00"), 10),
            ])
            result = api.getLastApiDate(facility)
        self.assertEqual(result, todt("2040-02-01 00:00:00"))

    def __test_getLastApiDateForOlderFacility(self):
        facility = self.mainFacility()
        otherfacility = self.otherFacility()
        api = self.createApi()
        with api:
            api.uploadProduction(facility, [
                (todt("2040-01-02 00:00:00"), 10),
            ])
            api.uploadProduction(otherfacility, [
                (todt("2040-01-03 00:00:00"), 10),
            ])
            result = api.getLastApiDate(facility)
        self.assertEqual(result, todt("2040-01-02 00:00:00"))

unittest.TestCase.__str__ = unittest.TestCase.id


# vim: ts=4 sw=4 et
