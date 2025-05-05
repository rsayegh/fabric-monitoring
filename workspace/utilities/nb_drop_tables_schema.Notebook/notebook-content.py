# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8f00ac4f-7c47-4094-9859-29635e022c8a",
# META       "default_lakehouse_name": "MonitoringLake",
# META       "default_lakehouse_workspace_id": "6db6ba4d-a289-4629-ac67-62735a6ca9bc",
# META       "known_lakehouses": [
# META         {
# META           "id": "8f00ac4f-7c47-4094-9859-29635e022c8a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

schema_name = 'staging'
tables = notebookutils.fs.ls(f"Tables/{schema_name}")
for table in tables:
    dropSql = f"DROP TABLE {schema_name}.{table.name}"
    spark.sql(dropSql)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
