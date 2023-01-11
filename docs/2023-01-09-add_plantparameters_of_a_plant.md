
To add the plant parameters of a plant you can run

```bash
PLANTMONITOR_MODULE_SETTINGS='conf.settings.prod' python addPlant.py conf/plants_plantparameters.yaml
```

where the plantmonitor_module_settings reads from .env.prod and the yaml looks like this:

```yaml
plants:
- plant:
    name: Alcolea
    plantParameters:
        peakPowerMWp: 2.16
        nominalPowerMW: 1.89
        connectionDate: 2016-01-01
        targetMonthlyEnergyGWh: 0
```

it will update the values if they exist.

You can find these values looking for `Càlcul Rendiment Planta_<plantname>` on our organization drive.

it can be even more complete, like this:

```yaml
municipalities:
- municipality:
    name: Cartagena
    ineCode: '30016'
    countryCode: ES
    country: España
    regionCode: '14'
    region: Murcia
    provinceCode: '30'
    province: Murcia
plants:
- plant:
    name: Asomada
    location:
        lat: 37.656603
        long: -0.940483
    plantParameters:
        peakPowerMWp: 3.998
        nominalPowerMW: 3.8
        connectionDate: 2022-12-01
        targetMonthlyEnergyGWh: 0.56558
    moduleParameters:
        nominalPowerMWp: 410
        efficiency: 20.2
        nModules: 9750
        degradation: 98.7
        Imp: 9.91
        Vmp: 41.4
        temperatureCoefficientI: 0.05
        temperatureCoefficientV: -0.29
        temperatureCoefficientPmax: -0.37
        irradiationSTC: 1000
        temperatureSTC: 25
        Voc: 49.3
        Isc: 10.41
```