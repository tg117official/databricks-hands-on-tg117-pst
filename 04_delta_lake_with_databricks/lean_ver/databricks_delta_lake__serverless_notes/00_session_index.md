# Databricks, Unity Catalog, and Delta Lake — Lean Serverless Notes

1. `00_setup_databricks_credentials_external_location.md`
2. `01_databricks_unity_catalog_delta_lake_foundations.md`
3. `02_unity_catalog_permissions.md`
4. `03_unity_catalog_paths_lifecycle_files_to_gold.md`
5. `04_delta_transaction_log_acid_time_travel_merge.md`
6. `05_delta_acid_schema_enforcement_checkpoints.md`
7. `06_delta_maintenance_optimize_time_travel_restore_vacuum.md`
8. `07_delta_schema_evolution_versioned_migration.md`
9. `08_delta_data_skipping_partitioning.md`
10. `09_delta_partitioning_edge_cases.md`
11. `10_delta_zorder_query_optimization.md`
12. `11_delta_liquid_clustering.md`

Use serverless notebook compute for Python and SQL cells. The external-location exercises use:

```text
abfss://data@demodb117.dfs.core.windows.net/
```

Run each session in sequence. Every session uses its own catalog, schema, table, or ADLS directory where practical.
