
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