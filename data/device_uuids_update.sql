--meters
UPDATE meter SET device_uuid = '2df182f8-ce0d-409f-bf8a-00282f8a1569' WHERE plant=6 --florida
UPDATE meter SET device_uuid = '716015b3-3f17-47e9-8c64-b728bfbec467' WHERE plant=22 --llanillos
UPDATE meter SET device_uuid = '529d55a4-26fc-4055-b268-ed51d197ff1b' WHERE plant=7 --matallana/exiom
UPDATE meter SET device_uuid = '5deb3780-02e2-4dcf-a6a7-de646991762c' WHERE plant=13  --tahal
UPDATE meter SET device_uuid = '08bc8d88-4ea2-4ee9-b817-00e1f07debba' WHERE plant=1   --vallhermoso/alcolea
UPDATE meter SET device_uuid = 'c5ed9fab-e73c-40a5-acb3-113e22052fd6' WHERE plant=40   --asomada


--inverters
----florida
INSERT INTO public.inverter(name, plant, brand, model, nominal_power_w, device_uuid)
	VALUES ('inverter1', 6, NULL, NULL, NULL, '13427caf-c0de-4a83-b68a-56d8caf91b80')
INSERT INTO public.inverter(name, plant, brand, model, nominal_power_w, device_uuid)
	VALUES ('inverter2', 6, NULL, NULL, NULL, 'c137a935-6049-427c-9ebd-b1e2a994b364')
----llanillos




--sensors
--Florida, inversor1, SensorTemperatureAmbient
UPDATE sensor SET device_uuid = '' WHERE id = 5
--Florida, inversor2, SensorTemperatureAmbient
UPDATE sensor SET device_uuid = '' WHERE id = 6