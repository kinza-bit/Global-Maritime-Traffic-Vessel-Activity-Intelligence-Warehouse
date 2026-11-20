\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS project_staging;
CREATE SCHEMA IF NOT EXISTS project_dw;
CREATE SCHEMA IF NOT EXISTS project_audit;
CREATE SCHEMA IF NOT EXISTS project_reporting;

\ir 01_staging_schema.sql
\ir 02_dw_schema.sql
\ir 03_load_staging_from_csv.sql
\ir 04_load_dimensions.sql
\ir 05_load_facts.sql
\ir 06_dq_checks.sql
\ir 07_reporting_views.sql