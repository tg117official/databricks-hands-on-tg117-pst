## 1. Check CLI

```powershell
where.exe databricks
databricks -v
```

Use Databricks CLI `0.205.0` or newer.

## 2. Create token

In Databricks:

```text
Settings → Developer → Access tokens → Generate new token
```

Copy the token.

## 3. Connect

```powershell
databricks configure --host "https://adb-<workspace-id>.<number>.azuredatabricks.net"
```

Paste the token when prompted.

Verify:

```powershell
databricks current-user me
```

## 4. Create JSON

Replace the placeholders and run:

```powershell
@'
{
  "azure_managed_identity": {
    "access_connector_id": "/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Databricks/accessConnectors/<connector-name>"
  }
}
'@ | Set-Content -Path .\credential.json -Encoding ascii
```

For a system-assigned identity, do not add `managed_identity_id`.

## 5. Create credential

```powershell
databricks storage-credentials create demodb117_storage_credential --json "@credential.json"
```

Verify:

```powershell
databricks storage-credentials get demodb117_storage_credential
```

## 6. Validate credential

Run after Hierarchical Namespace is enabled:

```powershell
databricks storage-credentials validate --storage-credential-name demodb117_storage_credential --url "abfss://data@demodb117.dfs.core.windows.net/"
```

## 7. Create location

```powershell
databricks external-locations create demodb117_data_location "abfss://data@demodb117.dfs.core.windows.net/" demodb117_storage_credential --comment "Auto Loader source location"
```

Verify:

```powershell
databricks external-locations get demodb117_data_location
```

## 8. Test

Run in a Databricks notebook:

```python
dbutils.fs.put(
    "abfss://data@demodb117.dfs.core.windows.net/cli_test/test.txt",
    "External location is working.",
    True
)
```

```python
display(
    dbutils.fs.ls(
        "abfss://data@demodb117.dfs.core.windows.net/cli_test/"
    )
)
```

```python
print(
    dbutils.fs.head(
        "abfss://data@demodb117.dfs.core.windows.net/cli_test/test.txt"
    )
)
```

Expected output:

```text
External location is working.
```
