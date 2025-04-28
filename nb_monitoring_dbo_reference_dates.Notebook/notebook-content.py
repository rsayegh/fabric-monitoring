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

# MARKDOWN ********************

# **Spark configuration**

# CELL ********************

# v-oder optimization
spark.conf.set("sprk.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# Dynamic Partition Overwrite to avoid deleting existing partitions
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# arrow enablement
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Helper notebook**

# CELL ********************

%run nb_helper

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Define a logging dataframe**

# CELL ********************

dfLogging = pd.DataFrame(columns = ['LoadId','NotebookId', 'NotebookName', 'WorkspaceId', 'CellId', 'Timestamp', 'ElapsedTime', 'Message', 'ErrorMessage'])
vContext = mssparkutils.runtime.context
vNotebookId = vContext["currentNotebookId"]
vLogNotebookName = vContext["currentNotebookName"]
vWorkspaceId = vContext["currentWorkspaceId"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Parameters --> convert to code for debugging the notebook. otherwise, keep commented as parameters are passed from master notebook**

# CELL ********************

pLoadId = "1"
pDebugMode = "yes"
pStartDate = '2023-01-01'
pEndDate = '2030-12-31'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Imports**

# MARKDOWN ********************

# 
# from pyspark.sql import SparkSession
# import numpy as np
# from datetime import datetime, timedelta, time
# import sys

# MARKDOWN ********************

# **Time range**

# CELL ********************

try:
    # get the start date from query_start_report
    vStartDateQuery = f"SELECT CAST('{pStartDate}' AS DATE) AS Dates"
    vEndDateQuery = f"SELECT CAST('{pEndDate}' AS DATE) AS Dates"
    dfStartDate = spark.sql(vStartDateQuery)
    dfEndDate = spark.sql(vEndDateQuery)
    dfDate = dfStartDate.union(dfEndDate)

    # # convert to pandas
    dfDate = dfDate.toPandas()

    # put the min and max dates in variables, add one day to the endDate
    startDate = dfDate['Dates'].min()
    endDate = dfDate['Dates'].max() #+ datetime.timedelta(days=1)

    # print(startDate,endDate)

except Exception as e:
    vMessage = "failed - setting time range"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'Time range', datetime.now(), vMessage, str(e) ] 
    print(f"{vMessage}. exception", e)
    sys.exit(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function to generate the dates**

# CELL ********************

# function that takes a start and end date to genereate a series of dates with an interval
def generate_series(start, stop, interval):
    """
    :param start  - lower bound, inclusive
    :param stop   - upper bound, exclusive
    :interval int - increment interval in seconds
    """


    spark = SparkSession.builder.getOrCreate()

    # Determine start and stops in epoch seconds
    start, stop = spark.createDataFrame(
        [(start, stop)], ("start", "stop")
    ).select(
        [col(c).cast("timestamp").cast("long") for c in ("start", "stop")
    ]).first()
    # Create range with increments and cast to timestamp
    return spark.range(start, stop, interval).select(
        col("id").cast("timestamp").alias("DateAndTime")
    )
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reference dates**

# CELL ********************

# start timer 
start = timer()

try:
    # generate the date series with an interval of 15min 
    df_reference_date = generate_series(startDate, endDate, 60 * 15)

    # convert to pandas
    df_reference_date = df_reference_date.toPandas()

    # enrich the table with additional columns
    df_reference_date['Date'] = df_reference_date['DateAndTime'].dt.date
    df_reference_date['DateKey'] = df_reference_date['DateAndTime'].dt.strftime('%Y%m%d').astype(int)
    df_reference_date['MonthKey'] = df_reference_date['DateAndTime'].dt.strftime('%Y%m%d').astype(str).str[:6]
    df_reference_date['Year'] = df_reference_date['DateAndTime'].dt.year
    df_reference_date['Quarter'] = 'Q' + df_reference_date['DateAndTime'].dt.quarter.astype(str)
    df_reference_date['Month'] = df_reference_date['DateAndTime'].dt.month
    df_reference_date['Day'] = df_reference_date['DateAndTime'].dt.day
    df_reference_date['IsWholeHour'] = np.where(df_reference_date['DateAndTime'].dt.strftime('%M') == '00' , True, False)

    # perform the conversion of columns
    df_reference_date = df_reference_date.astype({
            "DateKey" : "int32",
            "MonthKey" : "int32",
            "Year": "int32",
            "Month": "int32",
            "Day": "int32"
        })

    # convert from pandas to spark 
    spark_df_reference_date = spark.createDataFrame(df_reference_date) 

    # save to target
    spark_df_reference_date.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("dbo.reference_date")

    # logging
    end = timer()
    vElapsedTime = timedelta(seconds=end-start)
    vMessage = "reference dates succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'reference dates', datetime.now(), vElapsedTime, vMessage, ''] 

except Exception as e:
    # logging
    end = timer()
    vElapsedTime = timedelta(seconds=end-start)
    vMessage = "reference dates failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'reference dates', datetime.now(), vElapsedTime, vMessage, str(e) ] 
    print(f"{vMessage}. exception", str(e))
    sys.exit(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Logging**

# CELL ********************

try:
    # perform the conversion of columns
    dfLogging = dfLogging.astype({
            "LoadId": "string",	
            "NotebookId": "string", 	
            "NotebookName": "string", 
            "WorkspaceId": "string", 
            "CellId": "string", 
            "Timestamp": "datetime64[ns]", 
            "ElapsedTime": "string", 
            "Message": "string", 
            "ErrorMessage" : "string"
        })

    # save panda dataframe to a spark dataframe 
    sparkDF = spark.createDataFrame(dfLogging) 

    # save to the lakehouse
    
    sparkDF.write.mode("overwrite").format("delta").partitionBy("LoadId","NotebookName").option("mergeSchema", "true").saveAsTable("dbo.notebook_logging")
except Exception as e:
    vMessage = f"saving logs to the lakehouse failed. exception: {str(e)}"
    if pDebugMode == "yes":
        print(vMessage)    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
