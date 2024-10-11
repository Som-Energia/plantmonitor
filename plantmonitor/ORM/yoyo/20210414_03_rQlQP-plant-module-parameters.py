"""
Plant module parameters
"""

from yoyo import step

__depends__ = {'20210414_02_TgAMy-timescale'}

steps = [
    step("""\
        CREATE TABLE "plantmoduleparameters" (
          "id" SERIAL PRIMARY KEY,
          "plant" INTEGER NOT NULL,
          "n_modules" INTEGER NOT NULL,
          "degradation_cpercent" INTEGER NOT NULL,
          "max_power_current_ma" INTEGER NOT NULL,
          "max_power_voltage_mv" INTEGER NOT NULL,
          "current_temperature_coefficient_mpercent_c" INTEGER NOT NULL,
          "voltage_temperature_coefficient_mpercent_c" INTEGER NOT NULL,
          "standard_conditions_irradiation_w_m2" INTEGER NOT NULL,
          "standard_conditions_temperature_dc" INTEGER NOT NULL,
          "opencircuit_voltage_mv" INTEGER NOT NULL,
          "shortcircuit_current_ma" INTEGER NOT NULL
        );

        CREATE INDEX "idx_plantmoduleparameters__plant" ON "plantmoduleparameters" ("plant");

        ALTER TABLE "plantmoduleparameters" ADD CONSTRAINT "fk_plantmoduleparameters__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id");
        """,
        """
        DROP TABLE "plantmoduleparameters";
        """
    ),
]
