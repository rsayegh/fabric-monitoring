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

# **Parameters**

# CELL ********************

pLoadId = "1"
pToken = ""
pDebugMode = "yes"
pTopN = '0'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Variables**

# CELL ********************

vApiVersion = "v1.0"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# **Base URL and Header**

# CELL ********************

vBaseUrl = f'https://api.powerbi.com/{vApiVersion}/myorg/'
vHeader = {'Authorization': f'Bearer {vAccessToken}'} #, "Accept": None}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Required dataframes**

# CELL ********************

dfDatasetRefreshAttempts = pd.DataFrame()
dfDatasetRefreshHistory = pd.DataFrame()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Input for API**

# CELL ********************

try:

    # select datasets in active workspaces
    dfDatasetsTemp = spark.sql("SELECT ws.WorkspaceId, ds.DatasetsId FROM MonitoringLake.staging.datasets ds INNER JOIN MonitoringLake.staging.workspaces ws ON ws.WorkspaceId = ds.WorkspaceId WHERE ws.State = 'Active'")
    
    # convert to pandas
    dfDatasets = dfDatasetsTemp.select("*").toPandas()


    # # get the access token 
    # if pDebugMode == "yes":

    #     # read the staging csv file 
    #     dfDatasets = pd.read_csv("/lakehouse/default/" + "Files/Staging/datasets.csv")

    # else:

    #     # select datasets in active workspaces
    #     dfDatasetsTemp = spark.sql("SELECT ws.WorkspaceId, ds.DatasetsId FROM MonitoringLake.staging.datasets ds INNER JOIN MonitoringLake.staging.workspaces ws ON ws.WorkspaceId = ds.WorkspaceId WHERE ws.State = 'Active'")
        
    #     # convert to pandas
    #     dfDatasets = dfDatasetsTemp.select("*").toPandas()


    # logging
    vMessage = "input datasets for api call succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vNotebookName, vWorkspaceId, 'input datasets for api call', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "input datasets for api call failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vNotebookName, vWorkspaceId, 'input datasets for api call', datetime.now(), None, vMessage, str(e) ] 
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

# **Dataset refresh history**

# CELL ********************

# if a top n is not used
if pTopN == '0':
    vRefreshUrl = f"refreshes"
# if a top n is used
else:
    vRefreshUrl = f"refreshes?$top={pTopN}"

try:

    # if there are datasets to query
    if not dfDatasets.empty:

        # iterate over the input
        for i in dfDatasets.index:

            # start timer 
            start1 = timer()

            # get the target capacity and workspace id
            vWorkspaceId = dfDatasets['WorkspaceId'][i]
            vDatasetsId = dfDatasets['DatasetsId'][i]


            # dataset url
            vDatasetUrl = "groups/" + vWorkspaceId + "/datasets/" + vDatasetsId + "/" + vRefreshUrl

            # get the response
            response = requests.get(vBaseUrl + vDatasetUrl, headers=vHeader)

            # print(response)

            if response.status_code == 200:

                content = json.loads(response.content)
                current_count = len(content['value'])
                # print(current_count)

                # extract the dataset refresh history from the content
                try:
                    # flatten the value
                    dataset_refresh_history = pd.concat([pd.json_normalize(x) for x in content['value']])

                    # append to the dfDatasetsRefreshHistory dataframe
                    dfDatasetRefreshHistory = pd.concat([dfDatasetRefreshHistory, dataset_refresh_history], ignore_index=True)

                    vMessage = f"found {current_count} records for dataset {vDatasetsId}"
                    if pDebugMode == "yes":
                        print(vMessage)

                except Exception as e:
                    vMessage = f"no refresh history found for dataset {vDatasetsId}. exception: {str(e)}"
                    if pDebugMode == "yes":
                        print(vMessage)

                # logging
                end = timer()
                vElapsedTime = timedelta(seconds=end-start1)
                dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vNotebookName, vWorkspaceId, 'Dataset Refresh History', datetime.now(), vElapsedTime, vMessage, '']
            
      
    else:
        print(f"dataFrame dfDatasets is empty.")

    # logging
    vMessage = "extracting dataset refresh history succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vNotebookName, vWorkspaceId, 'extracting dataset refresh history', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "extracting dataset refresh history failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vNotebookName, vWorkspaceId, 'extracting dataset refresh history', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Refresh attempts**

# CELL ********************

try:

    # if there are datasets to query
    if not dfDatasetRefreshHistory.empty:

        # refresh attempts
        dfDatasetRefreshAttempts = flatten_nested_json_df(dfDatasetRefreshHistory[['requestId', 'refreshAttempts']].explode('refreshAttempts').dropna())

        # logging
        vMessage = "extracting dataset refresh attempts succeeded"
        dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vNotebookName, vWorkspaceId, 'extracting dataset refresh attempts', datetime.now(), None, vMessage, ''] 

    else:
        print(f"dataFrame dfDatasetRefreshHistory is empty.")

except Exception as e:
    vMessage = "extracting dataset refresh attempts failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vNotebookName, vWorkspaceId, 'extracting dataset refresh attempts', datetime.now(), None, vMessage, str(e) ] 
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
    dfDatasetRefreshAttempts,
    dfDatasetRefreshHistory
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
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vNotebookName, vWorkspaceId, 'formating column names', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "formating column names failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vNotebookName, vWorkspaceId, 'formating column names', datetime.now(), None, vMessage, str(e) ] 
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
        case "dfDatasetRefreshHistory":
            return "dataset_refresh_history"
        case "dfDatasetRefreshAttempts":
            return "dataset_refresh_attempts"


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
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vNotebookName, vWorkspaceId, 'saving delta tables', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "saving delta tables failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vNotebookName, vWorkspaceId, 'saving delta tables', datetime.now(), None, vMessage, str(e) ] 
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

# CELL ********************

# top_n = 50
# hists = []

# total_datasets = len(datasets)
# dataset_index = 0

# for dataset in datasets:
#     hist = requests.get(
#         f"{config['ApiGatewayUri']}/v1.0/myorg/groups/{dataset['workspaceId']}/datasets/{dataset['id']}/refreshes/?$top={top_n}",
#         headers={"Authorization": f"Bearer {config['AccessToken']}"}
#     )

#     if hist.status_code == 200:
#         hist_json = hist.json()
#         for item in hist_json['value']:
#             item['DatasetID'] = dataset['id']
#             item['DatasetName'] = dataset['name']
#             item['WorkspaceID'] = dataset['workspaceId']
#         hists.extend(hist_json['value'])

#     dataset_index += 1
#     print(f"Retrieving refresh history for {total_datasets} datasets. To skip this step, set ExportRefreshes to False and rerun the script",
#             f"Progress: {dataset_index * 100.0 / total_datasets}%")

# df = pd.DataFrame(hists)
# df = df.rename(columns={"id": "id", "refreshType": "refreshType", "startTime": "startTime",
#                         "endTime": "endTime", "serviceExceptionJson": "serviceExceptionJson",
#                         "status": "status", "DatasetID": "DatasetID", "WorkspaceID": "WorkspaceID",
#                         "DatasetName": "DatasetName"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
