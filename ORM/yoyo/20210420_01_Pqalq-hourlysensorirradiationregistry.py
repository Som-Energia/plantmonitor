"""
hourlysensorirradiationregistry
"""

from yoyo import step

__depends__ = {'20210414_03_rQlQP-plant-module-parameters'}

steps = [
    step('''
        DROP TABLE "integratedirradiationregistry"; 
        
        CREATE TABLE "hourlysensorirradiationregistry" (
           "sensor" INTEGER NOT NULL,
           "time" TIMESTAMP WITH TIME ZONE NOT NULL,
           "integratedirradiation_wh_m2" BIGINT,
           "expected_energy_wh" BIGINT,
           PRIMARY KEY ("sensor", "time")
         );
        
        ALTER TABLE "hourlysensorirradiationregistry" ADD CONSTRAINT "fk_hourlysensorirradiationregistry__sensor" FOREIGN KEY ("sensor") REFERENCES "sensor" ("id") ON DELETE CASCADE;
        ''', '''
        DROP TABLE "hourlysensorirradiationregistry"; 
        
        CREATE TABLE "integratedirradiationregistry" (
           "sensor" INTEGER NOT NULL,
           "time" TIMESTAMP WITH TIME ZONE NOT NULL,
           "integratedirradiation_wh_m2" BIGINT,
           PRIMARY KEY ("sensor", "time")
         );
        
        ALTER TABLE "integratedirradiationregistry" ADD CONSTRAINT "fk_integratedirradiationregistry__sensor" FOREIGN KEY ("sensor") REFERENCES "sensor" ("id") ON DELETE CASCADE;
    '''),
]
