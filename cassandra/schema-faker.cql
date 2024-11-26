USE kafkapipeline;
CREATE TABLE IF NOT EXISTS kafkapipeline.fakerdata (
  name TEXT,
  building_number TEXT,
  street_name TEXT,
  city TEXT,
  country TEXT,
  postcode TEXT,
  year INT,
  job TEXT,
  company TEXT,
  company_email TEXT,
  ssn TEXT,
  passport_number TEXT,
  credit_card_provider TEXT,
  credit_card_number TEXT,
  credit_card_expire TEXT,
  credit_card_security_code TEXT,
  PRIMARY KEY (name)
);

CREATE TABLE IF NOT EXISTS kafkapipeline.lapsummary (
  session_name TEXT,
  track TEXT,
  date TEXT,
  driver TEXT,
  driver_name TEXT,
  team TEXT,
  lap_number INT,
  lap_time TEXT,
  position INT,
  tyre_compound TEXT,
  tyre_life INT,
  pit_stop BOOLEAN,
  stint INT,
  fresh_tyre BOOLEAN,
  sector1_time TEXT,
  sector2_time TEXT,
  sector3_time TEXT,
  PRIMARY KEY (session_name, driver, lap_number)
);