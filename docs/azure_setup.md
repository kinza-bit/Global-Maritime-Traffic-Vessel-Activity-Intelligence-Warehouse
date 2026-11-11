# Azure Setup (Member 1)

| Resource | Name |
|---|---|
| Resource group | rg-maritime-dwbi |
| Storage account (ADLS Gen2) | stmaritimeks |
| Containers | bronze, silver, audit |
| Key Vault | kv-maritime-ks |
| Databricks workspace | dbw-maritime-ks |
| Access connector | ac-maritime |
| Storage credential | cred-maritime |
| External locations | loc_bronze, loc_silver, loc_audit |
| Secret scope | maritime-kv |
| Service principal | sp-maritime-pipeline |

## Key Vault secrets (names only)
sp-client-id, sp-client-secret, sp-tenant-id, gfw-api-token

## Important: our workspace uses SERVERLESS compute
Our subscription only offers serverless compute, so there is no classic cluster.
Storage access works through the access connector (managed identity) with the
role "Storage Blob Data Contributor" on the storage account.
The Databricks app has the role "Key Vault Secrets User" on the Key Vault.

## How to test
Run notebook 00_setup_check on Serverless. It must print a secret length,
"read back: hello" and three "OK: True" lines.

## Storage paths
abfss://<container>@stmaritimeks.dfs.core.windows.net/




# Compute settings
- Compute type: Serverless (no classic cluster available on our subscription)
- Spark config for service principal is NOT used. Storage access is through
  the access connector and external locations.
- Serverless costs money only while a notebook or job is running.
  Do not leave notebooks running.