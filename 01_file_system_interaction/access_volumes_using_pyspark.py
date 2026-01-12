df = spark.read.option("inferSchema", True).option("header", True).csv("/Volumes/demo_databricks_ws_7405615894876276/retaildata/my-retail-volume/cr.txt")
display(df)