CREATE SCHEMA IF NOT EXISTS project_staging
  MANAGED LOCATION 'abfss://silver@stmaritimeks.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS project_audit
  MANAGED LOCATION 'abfss://audit@stmaritimeks.dfs.core.windows.net/';