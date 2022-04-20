"""
Add planttargetmonthlyenergyhistoricmonthlyenergy table
"""

from yoyo import step

__depends__ = {'20211213_01_m7M7G-solar-events', '20220413_01_VgHU3-change-plantparameters-to-bigint'}


steps = [
    step('''
	CREATE TABLE "planttargetmonthlyenergyhistoricmonthlyenergy" (
	  "id" SERIAL PRIMARY KEY,
	  "plantparameters" INTEGER NOT NULL,
	  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
	  "monthly_target_energy_kwh" BIGINT NOT NULL,
	  "monthly_historic_energy_kwh" BIGINT NOT NULL
	);

	CREATE INDEX "idx_planttargetmonthlyenergyhistoricmonthlyenergy__plantparamet" 
	       ON "planttargetmonthlyenergyhistoricmonthlyenergy" ("plantparameters");

	ALTER TABLE "planttargetmonthlyenergyhistoricmonthlyenergy" 
	      ADD CONSTRAINT "fk_planttargetmonthlyenergyhistoricmonthlyenergy__plantparamete" 
	      FOREIGN KEY ("plantparameters") REFERENCES "plantparameters" ("id");
        ''','''
        DROP TABLE "planttargetmonthlyenergyhistoricmonthlyenergy";
        ''')
]
