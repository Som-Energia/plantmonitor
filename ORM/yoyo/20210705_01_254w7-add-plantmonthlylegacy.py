"""
Add PlantMonthlyLegacy
"""

from yoyo import step

__depends__ = {'20210517_01_uwtNO-expected-power-correction-factor-cpercent'}

steps = [
    step("""
        CREATE TABLE "plantmonthlylegacy" (
          "id" SERIAL PRIMARY KEY,
          "plant" INTEGER NOT NULL,
          "time" TIMESTAMP WITH TIME ZONE NOT NULL,
          "export_energy_wh" BIGINT NOT NULL
        );

        CREATE INDEX "idx_plantmonthlylegacy__plant" ON "plantmonthlylegacy" ("plant");

        ALTER TABLE "plantmonthlylegacy" ADD CONSTRAINT "fk_plantmonthlylegacy__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id");
    ""","""
        DROP TABLE "plantmonthlylegacy"
    """)
]
