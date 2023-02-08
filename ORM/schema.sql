-- Before commit changes in this file, create the corresponding migration

CREATE TABLE "forecastpredictor" (
  "id" SERIAL PRIMARY KEY,
  "name" TEXT UNIQUE NOT NULL
);

CREATE TABLE "forecastvariable" (
  "id" SERIAL PRIMARY KEY,
  "name" TEXT UNIQUE NOT NULL
);

CREATE TABLE "municipality" (
  "id" SERIAL PRIMARY KEY,
  "inecode" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "countrycode" TEXT,
  "country" TEXT,
  "regioncode" TEXT,
  "region" TEXT,
  "provincecode" TEXT,
  "province" TEXT
);

CREATE TABLE "plant" (
  "id" SERIAL PRIMARY KEY,
  "name" TEXT NOT NULL,
  "codename" TEXT NOT NULL,
  "municipality" INTEGER,
  "description" TEXT NOT NULL
);

CREATE INDEX "idx_plant__municipality" ON "plant" ("municipality");

ALTER TABLE "plant" ADD CONSTRAINT "fk_plant__municipality" FOREIGN KEY ("municipality") REFERENCES "municipality" ("id") ON DELETE SET NULL;

CREATE TABLE "anemometer" (
  "id" SERIAL PRIMARY KEY,
  "name" TEXT NOT NULL,
  "plant" INTEGER NOT NULL,
  "description" TEXT NOT NULL
);

CREATE INDEX "idx_anemometer__plant" ON "anemometer" ("plant");

ALTER TABLE "anemometer" ADD CONSTRAINT "fk_anemometer__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id") ON DELETE CASCADE;

CREATE TABLE "anemometerregistry" (
  "anemometer" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "wind_mms" BIGINT,
  PRIMARY KEY ("anemometer", "time")
);

ALTER TABLE "anemometerregistry" ADD CONSTRAINT "fk_anemometerregistry__anemometer" FOREIGN KEY ("anemometer") REFERENCES "anemometer" ("id") ON DELETE CASCADE;

CREATE TABLE "forecastmetadata" (
  "id" SERIAL PRIMARY KEY,
  "errorcode" TEXT NOT NULL,
  "plant" INTEGER NOT NULL,
  "variable" INTEGER,
  "predictor" INTEGER,
  "forecastdate" TIMESTAMP WITH TIME ZONE,
  "granularity" INTEGER
);

CREATE INDEX "idx_forecastmetadata__plant" ON "forecastmetadata" ("plant");

CREATE INDEX "idx_forecastmetadata__predictor" ON "forecastmetadata" ("predictor");

CREATE INDEX "idx_forecastmetadata__variable" ON "forecastmetadata" ("variable");

ALTER TABLE "forecastmetadata" ADD CONSTRAINT "fk_forecastmetadata__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id") ON DELETE CASCADE;

ALTER TABLE "forecastmetadata" ADD CONSTRAINT "fk_forecastmetadata__predictor" FOREIGN KEY ("predictor") REFERENCES "forecastpredictor" ("id") ON DELETE SET NULL;

ALTER TABLE "forecastmetadata" ADD CONSTRAINT "fk_forecastmetadata__variable" FOREIGN KEY ("variable") REFERENCES "forecastvariable" ("id") ON DELETE SET NULL;

CREATE TABLE "forecast" (
  "forecastmetadata" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "percentil10" INTEGER,
  "percentil50" INTEGER,
  "percentil90" INTEGER,
  PRIMARY KEY ("forecastmetadata", "time")
);

ALTER TABLE "forecast" ADD CONSTRAINT "fk_forecast__forecastmetadata" FOREIGN KEY ("forecastmetadata") REFERENCES "forecastmetadata" ("id") ON DELETE CASCADE;

CREATE TABLE "inclinometer" (
  "id" SERIAL PRIMARY KEY,
  "name" TEXT NOT NULL,
  "plant" INTEGER NOT NULL,
  "description" TEXT NOT NULL
);

CREATE INDEX "idx_inclinometer__plant" ON "inclinometer" ("plant");

ALTER TABLE "inclinometer" ADD CONSTRAINT "fk_inclinometer__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id") ON DELETE CASCADE;

CREATE TABLE "inclinometerregistry" (
  "inclinometer" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "angle_real_co" BIGINT,
  "angle_demand_co" BIGINT,
  PRIMARY KEY ("inclinometer", "time")
);

ALTER TABLE "inclinometerregistry" ADD CONSTRAINT "fk_inclinometerregistry__inclinometer" FOREIGN KEY ("inclinometer") REFERENCES "inclinometer" ("id") ON DELETE CASCADE;

CREATE TABLE "inverter" (
  "id" SERIAL PRIMARY KEY,
  "name" TEXT NOT NULL,
  "plant" INTEGER NOT NULL,
  "brand" TEXT,
  "model" TEXT,
  "nominal_power_w" INTEGER
);

CREATE INDEX "idx_inverter__plant" ON "inverter" ("plant");

ALTER TABLE "inverter" ADD CONSTRAINT "fk_inverter__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id") ON DELETE CASCADE;

CREATE TABLE "inverterregistry" (
  "inverter" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "power_w" BIGINT,
  "energy_wh" BIGINT,
  "intensity_cc_ma" BIGINT,
  "intensity_ca_ma" BIGINT,
  "voltage_cc_mv" BIGINT,
  "voltage_ca_mv" BIGINT,
  "uptime_h" BIGINT,
  "temperature_dc" BIGINT,
  "create_date" TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY ("inverter", "time")
);

ALTER TABLE "inverterregistry" ADD CONSTRAINT "fk_inverterregistry__inverter" FOREIGN KEY ("inverter") REFERENCES "inverter" ("id") ON DELETE CASCADE;

CREATE TABLE "marketrepresentative" (
  "id" SERIAL PRIMARY KEY,
  "name" TEXT NOT NULL,
  "plant" INTEGER NOT NULL,
  "description" TEXT NOT NULL
);

CREATE INDEX "idx_marketrepresentative__plant" ON "marketrepresentative" ("plant");

ALTER TABLE "marketrepresentative" ADD CONSTRAINT "fk_marketrepresentative__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id") ON DELETE CASCADE;

CREATE TABLE "marketrepresentativeregistry" (
  "marketrepresentative" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "billed_energy_wh" BIGINT,
  "billed_market_price_ce_mwh" BIGINT,
  "cost_deviation_ce" BIGINT,
  "absolute_deviation_ce" BIGINT,
  PRIMARY KEY ("marketrepresentative", "time")
);

ALTER TABLE "marketrepresentativeregistry" ADD CONSTRAINT "fk_marketrepresentativeregistry__marketrepresentative" FOREIGN KEY ("marketrepresentative") REFERENCES "marketrepresentative" ("id") ON DELETE CASCADE;

CREATE TABLE "meter" (
  "id" SERIAL PRIMARY KEY,
  "plant" INTEGER NOT NULL,
  "name" TEXT NOT NULL,
  "connection_protocol" TEXT DEFAULT 'ip' NOT NULL
);

CREATE INDEX "idx_meter__plant" ON "meter" ("plant");

ALTER TABLE "meter" ADD CONSTRAINT "fk_meter__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id") ON DELETE CASCADE;

CREATE TABLE "meterregistry" (
  "meter" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "export_energy_wh" BIGINT NOT NULL,
  "import_energy_wh" BIGINT NOT NULL,
  "r1_varh" BIGINT NOT NULL,
  "r2_varh" BIGINT NOT NULL,
  "r3_varh" BIGINT NOT NULL,
  "r4_varh" BIGINT NOT NULL,
  PRIMARY KEY ("meter", "time")
);

ALTER TABLE "meterregistry" ADD CONSTRAINT "fk_meterregistry__meter" FOREIGN KEY ("meter") REFERENCES "meter" ("id") ON DELETE CASCADE;

CREATE TABLE "nagios" (
  "id" SERIAL PRIMARY KEY,
  "name" TEXT NOT NULL,
  "plant" INTEGER NOT NULL,
  "description" TEXT NOT NULL
);

CREATE INDEX "idx_nagios__plant" ON "nagios" ("plant");

ALTER TABLE "nagios" ADD CONSTRAINT "fk_nagios__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id") ON DELETE CASCADE;

CREATE TABLE "nagiosregistry" (
  "nagios" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "status" TEXT NOT NULL,
  PRIMARY KEY ("nagios", "time")
);

ALTER TABLE "nagiosregistry" ADD CONSTRAINT "fk_nagiosregistry__nagios" FOREIGN KEY ("nagios") REFERENCES "nagios" ("id") ON DELETE CASCADE;

CREATE TABLE "omie" (
  "id" SERIAL PRIMARY KEY,
  "name" TEXT NOT NULL,
  "plant" INTEGER NOT NULL,
  "description" TEXT NOT NULL
);

CREATE INDEX "idx_omie__plant" ON "omie" ("plant");

ALTER TABLE "omie" ADD CONSTRAINT "fk_omie__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id") ON DELETE CASCADE;

CREATE TABLE "omieregistry" (
  "omie" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "price_ce_mwh" BIGINT,
  PRIMARY KEY ("omie", "time")
);

ALTER TABLE "omieregistry" ADD CONSTRAINT "fk_omieregistry__omie" FOREIGN KEY ("omie") REFERENCES "omie" ("id") ON DELETE CASCADE;

CREATE TABLE "plantlocation" (
  "id" SERIAL PRIMARY KEY,
  "plant" INTEGER NOT NULL,
  "latitude" DOUBLE PRECISION NOT NULL,
  "longitude" DOUBLE PRECISION NOT NULL
);

CREATE INDEX "idx_plantlocation__plant" ON "plantlocation" ("plant");

ALTER TABLE "plantlocation" ADD CONSTRAINT "fk_plantlocation__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id");

CREATE TABLE "plantmoduleparameters" (
  "id" SERIAL PRIMARY KEY,
  "plant" INTEGER NOT NULL,
  "brand" TEXT,
  "model" TEXT,
  "nominal_power_wp" INTEGER DEFAULT 250000 NOT NULL,
  "efficency_cpercent" INTEGER DEFAULT 1550 NOT NULL,
  "n_modules" INTEGER NOT NULL,
  "degradation_cpercent" INTEGER NOT NULL,
  "max_power_current_ma" INTEGER NOT NULL,
  "max_power_voltage_mv" INTEGER NOT NULL,
  "current_temperature_coefficient_mpercent_c" INTEGER NOT NULL,
  "voltage_temperature_coefficient_mpercent_c" INTEGER NOT NULL,
  "max_power_temperature_coefficient_mpercent_c" INTEGER DEFAULT -442 NOT NULL,
  "standard_conditions_irradiation_w_m2" INTEGER NOT NULL,
  "standard_conditions_temperature_dc" INTEGER NOT NULL,
  "opencircuit_voltage_mv" INTEGER NOT NULL,
  "shortcircuit_current_ma" INTEGER NOT NULL,
  "expected_power_correction_factor_cpercent" INTEGER DEFAULT 10000 NOT NULL
);

CREATE INDEX "idx_plantmoduleparameters__plant" ON "plantmoduleparameters" ("plant");

ALTER TABLE "plantmoduleparameters" ADD CONSTRAINT "fk_plantmoduleparameters__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id");

CREATE TABLE "plantmonthlylegacy" (
  "id" SERIAL PRIMARY KEY,
  "plant" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "export_energy_wh" BIGINT NOT NULL
);

CREATE INDEX "idx_plantmonthlylegacy__plant" ON "plantmonthlylegacy" ("plant");

ALTER TABLE "plantmonthlylegacy" ADD CONSTRAINT "fk_plantmonthlylegacy__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id");

CREATE TABLE "plantparameters" (
  "id" SERIAL PRIMARY KEY,
  "plant" INTEGER NOT NULL,
  "peak_power_w" BIGINT NOT NULL,
  "nominal_power_w" BIGINT NOT NULL,
  "connection_date" TIMESTAMP WITH TIME ZONE NOT NULL,
  "n_strings_plant" INTEGER,
  "n_strings_inverter" INTEGER,
  "n_modules_string" INTEGER,
  "inverter_loss_mpercent" INTEGER,
  "meter_loss_mpercent" INTEGER,
  "target_monthly_energy_wh" BIGINT NOT NULL,
  "historic_monthly_energy_wh" BIGINT,
  "month_theoric_pr_cpercent" BIGINT,
  "year_theoric_pr_cpercent" BIGINT
);

CREATE INDEX "idx_plantparameters__plant" ON "plantparameters" ("plant");

ALTER TABLE "plantparameters" ADD CONSTRAINT "fk_plantparameters__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id");

CREATE TABLE "plantestimatedmonthlyenergy" (
  "id" SERIAL PRIMARY KEY,
  "plantparameters" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "monthly_target_energy_kwh" BIGINT,
  "monthly_historic_energy_kwh" BIGINT
);

CREATE INDEX "idx_plantestimatedmonthlyenergy__plantparameters" ON "plantestimatedmonthlyenergy" ("plantparameters");

ALTER TABLE "plantestimatedmonthlyenergy" ADD CONSTRAINT "fk_plantestimatedmonthlyenergy__plantparameters" FOREIGN KEY ("plantparameters") REFERENCES "plantparameters" ("id") ON DELETE CASCADE;

CREATE TABLE "sensor" (
  "id" SERIAL PRIMARY KEY,
  "name" TEXT NOT NULL,
  "plant" INTEGER NOT NULL,
  "description" TEXT NOT NULL,
  "classtype" TEXT NOT NULL,
  "ambient" BOOLEAN
);

CREATE INDEX "idx_sensor__plant" ON "sensor" ("plant");

ALTER TABLE "sensor" ADD CONSTRAINT "fk_sensor__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id") ON DELETE CASCADE;

CREATE TABLE "hourlysensorirradiationregistry" (
  "sensor" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "integratedirradiation_wh_m2" BIGINT,
  "expected_energy_wh" BIGINT,
  PRIMARY KEY ("sensor", "time")
);

ALTER TABLE "hourlysensorirradiationregistry" ADD CONSTRAINT "fk_hourlysensorirradiationregistry__sensor" FOREIGN KEY ("sensor") REFERENCES "sensor" ("id") ON DELETE CASCADE;

CREATE TABLE "sensorirradiationregistry" (
  "sensor" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "irradiation_w_m2" BIGINT,
  "temperature_dc" BIGINT,
  "create_date" TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY ("sensor", "time")
);

ALTER TABLE "sensorirradiationregistry" ADD CONSTRAINT "fk_sensorirradiationregistry__sensor" FOREIGN KEY ("sensor") REFERENCES "sensor" ("id") ON DELETE CASCADE;

CREATE TABLE "sensortemperatureambientregistry" (
  "sensor" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "temperature_dc" BIGINT,
  "create_date" TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY ("sensor", "time")
);

ALTER TABLE "sensortemperatureambientregistry" ADD CONSTRAINT "fk_sensortemperatureambientregistry__sensor" FOREIGN KEY ("sensor") REFERENCES "sensor" ("id") ON DELETE CASCADE;

CREATE TABLE "sensortemperaturemoduleregistry" (
  "sensor" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "temperature_dc" BIGINT,
  "create_date" TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY ("sensor", "time")
);

ALTER TABLE "sensortemperaturemoduleregistry" ADD CONSTRAINT "fk_sensortemperaturemoduleregistry__sensor" FOREIGN KEY ("sensor") REFERENCES "sensor" ("id") ON DELETE CASCADE;

CREATE TABLE "simel" (
  "id" SERIAL PRIMARY KEY,
  "name" TEXT NOT NULL,
  "plant" INTEGER NOT NULL,
  "description" TEXT NOT NULL
);

CREATE INDEX "idx_simel__plant" ON "simel" ("plant");

ALTER TABLE "simel" ADD CONSTRAINT "fk_simel__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id") ON DELETE CASCADE;

CREATE TABLE "simelregistry" (
  "simel" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "exported_energy_wh" BIGINT,
  "imported_energy_wh" BIGINT,
  "r1_varh" BIGINT,
  "r2_varh" BIGINT,
  "r3_varh" BIGINT,
  "r4_varh" BIGINT,
  PRIMARY KEY ("simel", "time")
);

ALTER TABLE "simelregistry" ADD CONSTRAINT "fk_simelregistry__simel" FOREIGN KEY ("simel") REFERENCES "simel" ("id") ON DELETE CASCADE;

CREATE TABLE "solarevent" (
  "id" SERIAL PRIMARY KEY,
  "plant" INTEGER NOT NULL,
  "sunrise" TIMESTAMP WITH TIME ZONE NOT NULL,
  "sunset" TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX "idx_solarevent__plant" ON "solarevent" ("plant");

ALTER TABLE "solarevent" ADD CONSTRAINT "fk_solarevent__plant" FOREIGN KEY ("plant") REFERENCES "plant" ("id") ON DELETE CASCADE;

CREATE TABLE "string" (
  "id" SERIAL PRIMARY KEY,
  "inverter" INTEGER NOT NULL,
  "name" TEXT NOT NULL,
  "stringbox_name" TEXT
);

CREATE INDEX "idx_string__inverter" ON "string" ("inverter");

ALTER TABLE "string" ADD CONSTRAINT "fk_string__inverter" FOREIGN KEY ("inverter") REFERENCES "inverter" ("id") ON DELETE CASCADE;

CREATE TABLE "stringregistry" (
  "string" INTEGER NOT NULL,
  "time" TIMESTAMP WITH TIME ZONE NOT NULL,
  "intensity_ma" BIGINT,
  "create_date" TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY ("string", "time")
);

ALTER TABLE "stringregistry" ADD CONSTRAINT "fk_stringregistry__string" FOREIGN KEY ("string") REFERENCES "string" ("id") ON DELETE CASCADE