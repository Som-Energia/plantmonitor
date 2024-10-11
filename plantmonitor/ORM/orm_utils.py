#!/usr/bin/env python
import importlib

__all__ = [
    'str2model'
]

supported_models = [
    "Meter",
    "Inverter",
    "SensorIrradiation",
    "SensorTemperatureAmbient",
    "SensorTemperatureModule",
    "String"
]


def str2model(db, model_name):
    # module = importlib.import_module('ORM.models')

    return getattr(db, model_name) \
        if model_name in supported_models \
        else None
