"""
Change Optional to Set
"""

from yoyo import step

__depends__ = {'20220420_01_up8HQ-add-target-historic-energy-plant'}

steps = [
    step('''
    ALTER TABLE "plantestimatedmonthlyenergy" DROP CONSTRAINT "fk_plantestimatedmonthlyenergy__plantparameters";
    ALTER TABLE "plantestimatedmonthlyenergy" ADD CONSTRAINT "fk_plantestimatedmonthlyenergy__plantparameters" FOREIGN KEY ("plantparameters") REFERENCES "plantparameters" ("id") ON DELETE CASCADE;
    ''',
    '''
    ALTER TABLE "plantestimatedmonthlyenergy" DROP CONSTRAINT "fk_plantestimatedmonthlyenergy__plantparameters";
    ALTER TABLE "plantestimatedmonthlyenergy" ADD CONSTRAINT "fk_plantestimatedmonthlyenergy__plantparameters" FOREIGN KEY ("plantparameters") REFERENCES "plantparameters" ("id");
    ''')
]
