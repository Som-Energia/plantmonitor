"""
Add Inverter Strings tables
"""

from yoyo import step


__depends__ = {'20210615_02_0sxG6-alters-of-fixed-data', '20210625_01_ckjXc-meter-connection-protocol', '20210705_01_254w7-add-plantmonthlylegacy'}

steps = [
    step('''
        CREATE TABLE "string" (
          "id" SERIAL PRIMARY KEY,
          "inverter" INTEGER NOT NULL,
          "name" TEXT NOT NULL
        );

        CREATE INDEX "idx_string__inverter" ON "string" ("inverter");

        ALTER TABLE "string" ADD CONSTRAINT "fk_string__inverter" FOREIGN KEY ("inverter") REFERENCES "inverter" ("id") ON DELETE CASCADE;

        CREATE TABLE "stringregistry" (
          "string" INTEGER NOT NULL,
          "time" TIMESTAMP WITH TIME ZONE NOT NULL,
          "intensity_ma" BIGINT,
          PRIMARY KEY ("string", "time")
        );

        ALTER TABLE "stringregistry" ADD CONSTRAINT "fk_stringregistry__string" FOREIGN KEY ("string") REFERENCES "string" ("id") ON DELETE CASCADE
    ''','''
        DROP TABLE "string";
        DROP TABLE "stringregistry";
    ''')
]

