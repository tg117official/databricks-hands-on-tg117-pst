# Databricks CLI Setup: Storage Credential and External Location

Use Windows PowerShell and Databricks CLI `0.205.0` or newer.

## 1. Check the CLI

```powershell
where.exe databricks
databricks -v
databricks storage-credentials create --help
```

The help must show `create NAME [flags]`.

## 2. Create a token

In Databricks, open **Settings → Developer → Access tokens → Generate new token**.

## 3. Connect

```powershell
databricks configure --host "https://adb-<workspace-id>.<number>.azuredatabricks.net"
databricks current-user me
```

Paste the token when prompted.

## 4. Create the request file

```powershell
@'
{
  "azure_managed_identity": {
    "access_connector_id": "/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Databricks/accessConnectors/<connector-name>"
  }
}
'@ | Set-Content -Path .\credential.json -Encoding ascii
```

Do not add `managed_identity_id` for a system-assigned identity.

## 5. Create the credential

```powershell
databricks storage-credentials create demodb117_storage_credential --json "@credential.json"
databricks storage-credentials get demodb117_storage_credential
```

## 6. Validate ADLS

Run after Hierarchical Namespace is enabled and the Access Connector has `Storage Blob Data Contributor`:

```powershell
databricks storage-credentials validate --storage-credential-name demodb117_storage_credential --url "abfss://data@demodb117.dfs.core.windows.net/"
```

## 7. Create the location

```powershell
databricks external-locations create demodb117_data_location "abfss://data@demodb117.dfs.core.windows.net/" demodb117_storage_credential --comment "Databricks training location"
databricks external-locations get demodb117_data_location
```

## 8. Test

Run in a Databricks Python notebook:

```python
dbutils.fs.put(
    "abfss://data@demodb117.dfs.core.windows.net/cli_test/test.txt",
    "External location is working.",
    True,
)

display(
    dbutils.fs.ls(
        "abfss://data@demodb117.dfs.core.windows.net/cli_test"
    )
)

print(
    dbutils.fs.head(
        "abfss://data@demodb117.dfs.core.windows.net/cli_test/test.txt"
    )
)
```

Expected text: `External location is working.`
