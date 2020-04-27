from .meteologica_api_utils import (
    MeteologicaApi_Mock,
    MeteologicaApi,
    MeteologicaApiError,
)
from django.conf import settings
from pathlib import Path
from unittest.mock import patch
import unittest

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
            ("2040-01-01 00:00:00", 10),
        ])
        self.assertEqual(
            api.lastDateUploaded(facility),
            "2040-01-01 00:00:00"
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
            ("2040-01-01 00:00:00", 10),
            ("2040-01-02 00:00:00", 10),
        ])
        self.assertEqual(
            api.lastDateUploaded(facility),
            "2040-01-02 00:00:00"
        )

    def test_uploadProduction_calledTwice(self):
        facility = self.mainFacility()
        api = self.createApi()
        api.uploadProduction(facility, [
            ("2040-01-02 00:00:00", 10),
        ])
        api.uploadProduction(facility, [
            ("2040-01-01 00:00:00", 10),
        ])
        self.assertEqual(
            api.lastDateUploaded(facility),
            "2040-01-02 00:00:00"
        )

    def test_uploadProduction_doesNotChangeOtherFacility(self):
        facility = self.mainFacility()
        api = self.createApi()
        api.uploadProduction(facility, [
            ("2040-01-01 00:00:00", 10),
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
            ("2040-01-02 00:00:00", 10),
        ])
        api.uploadProduction(otherFacility, [
            ("2040-01-01 00:00:00", 10),
        ])
        self.assertEqual(
            api.lastDateUploaded(otherFacility),
            "2040-01-01 00:00:00"
        )

    def test_uploadProduction_wrongFacility(self):
        api = self.createApi()
        with self.assertRaises(MeteologicaApiError) as ctx:
            api.uploadProduction("WrongPlant", [
                ("2040-01-01 00:00:00", 10),
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
            ("2040-01-01 00:00:00", 10),
        ])
  


class MeteologicaApi_Test(MeteologicaApiMock_Test):

    def createApi(self, **kwds):
        params=dict(
            wsdl=settings.METEOLOGICA_CONF['wsdl'],
            username=settings.METEOLOGICA_CONF['username'],
            password=settings.METEOLOGICA_CONF['password'],
            lastDateFile='lastDateFile.yaml',
            showResponses=False,
        )
        params.update(kwds)
        return MeteologicaApi(**params)

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
            ("2040-01-01 00:00:00", 10),
        ])
        api2 = self.createApi()
        self.assertEqual(
            api2.lastDateUploaded(facility),
            "2040-01-01 00:00:00"
        )

    def test_session_unitializedOnConstruction(self):
        facility = self.mainFacility()
        api = self.createApi()
        self.assertFalse(api.session())
    
    def test_session_withinContextIsTrue(self):
        facility = self.mainFacility()
        api = self.createApi()
        with api:
            self.assertTrue(api.session()) 

    def test_session_outsideContextLogout(self):
        facility = self.mainFacility()
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
                ("2040-01-01 00:00:00", 10),
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




unittest.TestCase.__str__ = unittest.TestCase.id


# vim: ts=4 sw=4 et
