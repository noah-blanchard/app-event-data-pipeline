-- One schema per warehouse layer (see README, "Data model").
-- IF NOT EXISTS makes every file in sql/schema safe to run again.
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS quality;
