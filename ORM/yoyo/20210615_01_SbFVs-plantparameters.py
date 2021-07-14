"""
plantparameters
"""

from yoyo import step

__depends__ = {'20210517_01_uwtNO-expected-power-correction-factor-cpercent'}

steps = [
    step('''
        CREATE TABLE "plantparameters" (
        "id" SERIAL PRIMARY KEY,
        "plant" INTEGER NOT NULL,
        "peak_power_w" INTEGER NOT NULL,
        "nominal_power_w" INTEGER NOT NULL,
        "connection_date" TIMESTAMP WITH TIME ZONE NOT NULL,
        "n_strings_plant" INTEGER,
        "n_strings_inverter" INTEGER,
        "n_modules_string" INTEGER,
        "inverter_loss_mpercent" INTEGER,
        "meter_loss_mpercent" INTEGER,
        "target_monthly_energy_wh" INTEGER NOT NULL,
        "historic_monthly_energy_wh" INTEGER,
        "month_theoric_pr_cpercent" INTEGER,
        "year_theoric_pr_cpercent" INTEGER
        );

        CREATE INDEX "idx_plantparameters__plant" ON "plantparameters" ("plant");

        ALTER TABLE "plantparameters" ADD CONSTRAINT "fk_plantparameters__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id");
        ''', '''
        DROP TABLE "plantparameters";
    ''')
]
