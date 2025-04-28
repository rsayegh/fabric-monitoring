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
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Spark configuration**

# CELL ********************

# bronze configuration
spark.conf.set("spark.sql.parquet.vorder.enabled","false")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","false")
spark.conf.set("spark.databricks.delta.collect.stats","false")

# Dynamic Partition Overwrite to avoid deleting existing partitions
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

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

# MARKDOWN ********************

# pLoadId = "1"
# pToken = ""
# pDebugMode = "yes"

# MARKDOWN ********************

# **Access token**

# CELL ********************

vScope = "https://analysis.windows.net/powerbi/api"
# get the access token 
if pDebugMode == "yes":
    # in debug mode, use the token of the current user
    vAccessToken  = notebookutils.credentials.getToken(vScope)
else:
    # when the code is run from the pipelines, to token is generated in a previous step and passed as a parameter to the notebook
    vAccessToken = pToken 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Base URL and Header**

# CELL ********************

vApiVersion = "v1"
vBaseUrl = f"https://api.fabric.microsoft.com/{vApiVersion}/"
vHeaders = {'Authorization': f'Bearer {vAccessToken}'}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Required dataframes**

# CELL ********************

df_domains = pd.DataFrame()
df_external_data_shares = pd.DataFrame()
df_tenant_settings = pd.DataFrame()
df_capacities = pd.DataFrame()
df_connections = pd.DataFrame()
df_deployment_pipelines = pd.DataFrame()
df_gateways = pd.DataFrame()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function to get the api url**

# CELL ********************

def get_extraction_url(extraction):
    match extraction:           
        case "domains":
            return "admin/domains"

        case "external_data_shares":
            return "admin/items/externalDataShares"

        case "tenant_settings":
            return "admin/tenantsettings"

        case "capacities":
            return "capacities"

        case "connections":
            return "connections"

        case "deployment_pipelines":
            return "deploymentPipelines"

        case "gateways":
            return "gateways"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Extraction**

# CELL ********************

vExtractionList = [
    "domains", 
    "external_data_shares", 
    "tenant_settings", 
    "capacities", 
    "connections", 
    "deployment_pipelines", 
    "gateways"
    ]

# iterate through the list
for item in vExtractionList:

    # set the extraction type
    vExtractionType = item
    print(f"extracting {vExtractionType}")

    # set the dataframe name
    vDataframeName = f"df_{vExtractionType}"

    # set the extraction url
    vExtractionUrl = get_extraction_url(vExtractionType)

    # set the final url
    vUrl = vBaseUrl + vExtractionUrl

    # create the api global dataframe
    api_call_global_dataframe = pd.DataFrame()

    try:
        
        # make the api call
        api_call_main(vUrl, vHeaders, pDebugMode, vExtractionType)

        # concat to the correspondant dataframe
        if vDataframeName in globals():
            globals()[vDataframeName] = pd.concat([globals()[vDataframeName], api_call_global_dataframe], ignore_index=True)

        # logging
        vMessage = f"extracting {vExtractionType} succeeded"
        dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, f"extracting {vExtractionType}", datetime.now(), None, vMessage, ''] 

    except Exception as e:
        vMessage = f"extracting {vExtractionType} failed"
        dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, f"extracting {vExtractionType}", datetime.now(), None, vMessage, str(e) ] 
        if pDebugMode == "yes":
            print(str(e))
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **List of dataframes**

# CELL ********************

# create a list of dataframes
dfList = [
    df_domains,
    df_external_data_shares,
    df_tenant_settings,
    df_capacities,
    df_connections,
    df_deployment_pipelines,
    df_gateways
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Format column names**

# CELL ********************

# iterate over the dataframes
for i, df in enumerate(dfList):

    # get the column names
    columns = dfList[i].columns.values.tolist()

    # iterate over the column name and rename after capitalizin the first letter
    for columnName in columns:

        # split the column name, take the last item and upper case the first letter
        processed_column = process_column_name(columnName, '.')

        # # replace the column name in the dataframe
        # dfList[i].columns = dfList[i].columns.str.replace(columnName, processed_column, regex=True)

        # replace the column name in the dataframe
        dfList[i].rename(columns={columnName: processed_column}, inplace=True)

    # convert all columns to string
    all_columns = list(dfList[i]) # Creates list of all column headers
    dfList[i][all_columns] = dfList[i][all_columns].astype(str)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Delta table name**

# CELL ********************

# get original dataframe name
def get_df_name(df):
    name = [x for x in globals() if globals()[x] is df][0]
    return name

# get the delta table name
def get_delta_name(name):
    match name:           
        case "df_domains":
            return "domains"
        case "df_external_data_shares":
            return "external_data_shares"
        case "df_tenant_settings":
            return "tenant_settings"
        case "df_capacities":
            return "capacities"
        case "df_connections":
            return "connections"
        case "df_deployment_pipelines":
            return "deployment_pipelines"
        case "df_gateways":
            return "gateways"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Save to lakehouse**

# CELL ********************

# iterate over the dataframes
for i, df in enumerate(dfList):

    # get orifinal name of dataframe
    vOriginalDataframeName = get_df_name(dfList[i])

    # get the delta table name
    vDeltaTableName = get_delta_name(vOriginalDataframeName)

    # print(vOriginalDataframeName,vDeltaTableName)

    # check if current dataframe is not empty
    if not dfList[i].empty:
        
        # saving as delta
        try:

            # create the spark dataframe
            sparkDF = spark.createDataFrame(dfList[i]) 

            # save to the lakehouse
            sparkDF.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"staging.{vDeltaTableName}")

            # logging
            vMessage = f"saving {vDeltaTableName} succeeded"
            dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, f"saving {vDeltaTableName}", datetime.now(), None, vMessage, ''] 

        except Exception as e:
            vMessage = f"saving {vDeltaTableName} failed"
            dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, f"saving {vDeltaTableName}", datetime.now(), None, vMessage, str(e) ] 
            if pDebugMode == "yes":
                print(vMessage)
                print(str(e))
    else:
        print(f"dataFrame {vOriginalDataframeName} is empty!")



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
    sparkDF.write.mode("overwrite").format("delta").partitionBy("LoadId","NotebookName").option("mergeSchema", "true").saveAsTable("staging.notebook_logging")

except Exception as e:
    vMessage = f"saving logs to the lakehouse failed. exception: {str(e)}"
    if pDebugMode == "yes":
        print(vMessage)    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
