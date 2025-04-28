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

# **Parameters --> convert to code for debugging the notebook. otherwise, keep commented as parameters are passed from master notebook**

# MARKDOWN ********************

# pLoadId = "1"
# pToken = ""
# pDebugMode = "yes"

# MARKDOWN ********************

# **Define a logging empty dataframe**

# CELL ********************

dfLogging = pd.DataFrame(columns = ['LoadId','NotebookId', 'NotebookName', 'WorkspaceId', 'CellId', 'Timestamp', 'ElapsedTime', 'Message', 'ErrorMessage'])
vContext = mssparkutils.runtime.context
vNotebookId = vContext["currentNotebookId"]
vLogNotebookName = vContext["currentNotebookName"]
vWorkspaceId = vContext["currentWorkspaceId"] # where the notebook is running, to not confuse with source and target workspaces

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Access token**

# CELL ********************

vScope = "https://analysis.windows.net/powerbi/api"
# get the access token 
if pDebugMode == "yes":
    # in debug mode, use the token of the current user
    vAccessToken  = mssparkutils.credentials.getToken(vScope)
else:
    # when the code is run from the pipelines, to token is generated in a previous step and passed as a parameter to the notebook
    vAccessToken = pToken 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Base url and header**

# CELL ********************

vApiVersion = "v2.0" #to extract gateways, the api v2 is required
base_url = f'https://api.powerbi.com/{vApiVersion}/myorg/'
header = {'Content-Type':'application/json','Authorization': f'Bearer {vAccessToken}'}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **API call**

# CELL ********************

try:

    # url 
    vGatewaysUrl = f"{base_url}gatewayClusters?`$expand=memberGateways"

    # response
    gateways_response = requests.get(url=vGatewaysUrl, headers=header)

    # response as json
    gateways_response_json = json.loads(gateways_response.content)

    # logging
    vMessage = "gateway clusters response succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'gateway clusters response', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "gateway clusters response failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'gateway clusters response', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))

    # if the extraction fails, exit
    sys.exit(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Required dataframes**

# CELL ********************

df_gateway_clusters = pd.DataFrame()
df_gateway_cluster_members = pd.DataFrame()
df_gateway_cluster_member_annotations = pd.DataFrame()
df_gateway_cluster_permissions = pd.DataFrame()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Gateway Clusters**

# CELL ********************

try:

    df_gateway_clusters_temp = pd.concat([pd.json_normalize(x) for x in gateways_response_json['value']])
    df_gateway_clusters = pd.concat([df_gateway_clusters, df_gateway_clusters_temp], ignore_index=True)


    # logging
    vMessage = "extracting gateway clusters succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'gateway clusters', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "extracting gateway clusters failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'gateway clusters', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Permissions**

# CELL ********************

if not df_gateway_clusters.empty:
    try:

        df_gateway_cluster_permissions_temp = flatten_nested_json_df(df_gateway_clusters[['id', 'permissions']].explode('permissions').dropna())
        df_gateway_cluster_permissions = pd.concat([df_gateway_cluster_permissions, df_gateway_cluster_permissions_temp], ignore_index=True)

        # logging
        vMessage = "extracting gateway cluster permissions succeeded"
        dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'gateway cluster permissions', datetime.now(), None, vMessage, ''] 

    except Exception as e:
        vMessage = "extracting gateway cluster permissions failed"
        dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'gateway cluster permissions', datetime.now(), None, vMessage, str(e) ] 
        if pDebugMode == "yes":
            print(str(e))
else:
    vMessage = "df_gateway_clusters dataframe does not exist"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'gateway cluster permissions', datetime.now(), '', vMessage, ''] 
    if pDebugMode == "yes":
        print(vMessage)                

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Members**

# CELL ********************

if not df_gateway_clusters.empty:
    try:

        # flatten the memberGateways node
        df_gateway_cluster_members_temp = flatten_nested_json_df(df_gateway_clusters[['id', 'memberGateways']].explode('memberGateways').dropna())

        df_gateway_cluster_members = pd.concat([df_gateway_cluster_members, df_gateway_cluster_members_temp], ignore_index=True)


        # logging
        vMessage = "extracting gateway cluster members succeeded"
        dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'gateway cluster members', datetime.now(), None, vMessage, ''] 

    except Exception as e:
        vMessage = "extracting gateway cluster members failed"
        dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'gateway cluster members', datetime.now(), None, vMessage, str(e) ] 
        if pDebugMode == "yes":
            print(str(e))
else:
    vMessage = "df_gateway_clusters dataframe does not exist"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'gateway cluster members', datetime.now(), '', vMessage, ''] 
    if pDebugMode == "yes":
        print(vMessage)                

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
    df_gateway_clusters,
    df_gateway_cluster_permissions,
    df_gateway_cluster_members
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Format column names**

# CELL ********************

try:
    # iterate over the dataframes
    for i, df in enumerate(dfList):


        # get the column names
        columns = dfList[i].columns.values.tolist()

        # iterate over the column name and rename after capitalizin the first letter
        for columnName in columns:

            # split the column name, take the last item and upper case the first letter
            processed_column = process_column_name(columnName, '.')

            dfList[i].rename(columns={columnName: processed_column}, inplace=True)

    # logging
    vMessage = "formating column names succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'formating column names', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "formating column names failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'formating column names', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function to set table names**

# CELL ********************

# get original dataframe name
def get_df_name(df):
    name = [x for x in globals() if globals()[x] is df][0]
    return name

# get the delta table name
def get_delta_name(name):
    match name:       

        case "df_gateway_clusters":
            return "gateway_clusters"

        case "df_gateway_cluster_permissions":
            return "gateway_cluster_permissions"

        case "df_gateway_cluster_members":
            return "gateway_cluster_members"            

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Saving as delta tables**

# CELL ********************

try:
    # iterate over the dataframes
    for i, df in enumerate(dfList):

        # get orifinal name of dataframe
        vOriginalDataframeName = get_df_name(dfList[i])


        # Creates list of all column headers
        all_columns = list(dfList[i]) 
        
        # convert to string
        dfList[i][all_columns] = dfList[i][all_columns].astype(str)


        # get the delta table name
        vDeltaTableName = get_delta_name(vOriginalDataframeName)
        print(vOriginalDataframeName,vDeltaTableName)


        # check if current dataframe is not empty
        if not dfList[i].empty:
            
            # saving as delta
            try:
    
                # save panda dataframe to a spark dataframe 
                vMessage = f"saving {vDeltaTableName} spark dataframe to delta table"
                if pDebugMode == "yes":
                    print(vMessage)
                
                sparkDF = spark.createDataFrame(dfList[i]) 

                # save to the lakehouse
                sparkDF.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"staging.{vDeltaTableName}")

            except Exception as e:
                vMessage = f"failed saving {vDeltaTableName} spark dataframe to delta table. exception: {str(e)}"
                if pDebugMode == "yes":
                    print(vMessage)
            
        else:
            print(f"dataFrame {vOriginalDataframeName} is empty!")

    # logging
    vMessage = "saving delta tables succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'saving delta tables', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "saving delta tables failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'saving delta tables', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))

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
