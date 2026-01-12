storage_account = "pstdatademo"
container = "bronze"
file_path = "customers/customers.csv"

abfss_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{file_path}"

# For quick testing (avoid hardcoding in real projects)

spark.conf.set(
  f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
  ""
)

df = spark.read.option("inferSchema","true").option("header","true").csv(abfss_path)
display(df)
