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

# **Libraries**

# CELL ********************

import time
import datetime as dt
from datetime import datetime, timedelta 
import numpy as np

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
# pDebugMode = "no"
# pInitialize = 'yes'
# pExportInventoryExpressions = 'yes'  
# pThrottleScanApi = 'yes' 
# pDateAndTime = '2024-01-01 00:00:00'


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
vContext = notebookutils.runtime.context
vNotebookId = vContext["currentNotebookId"]
vLogNotebookName = vContext["currentNotebookName"]
vWorkspaceId = vContext["currentWorkspaceId"]


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

# **Format Date**

# CELL ********************

try:
    # date format
    vDateFormat = '%Y-%m-%d %H:%M:%S'

    # current start in iso8601
    vCurrentStart = datetime.strptime(pDateAndTime,vDateFormat)
    vCurrentStartIso8601 = vCurrentStart.strftime('%Y-%m-%dT%H:%M:%S.%f%Z')+'0Z'

    # print(vCurrentStartIso8601)

except Exception as e:
    # logging
    vMessage = "failed - date formating "
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'Format Date', datetime.now(), vMessage, str(e) ] 
    print(f"{vMessage}. exception", e)
    sys.exit(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Base URL and Header**

# CELL ********************

vBaseUrl = f'https://api.powerbi.com/{vApiVersion}/myorg/'
vHeaders = {'Authorization': f'Bearer {vAccessToken}'} #, "Accept": None}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Required dataframes**

# CELL ********************

dfDashboards = pd.DataFrame() 
dfDashboardUsers = pd.DataFrame() 
dfDataAgents = pd.DataFrame() 
dfDataAgentUsers = pd.DataFrame() 
dfDataFactoryPipelineRelations = pd.DataFrame() 
dfDataFactoryPipelines = pd.DataFrame() 
dfDataFactoryPipelineUsers = pd.DataFrame() 
dfDataflows = pd.DataFrame() 
dfDataflowUsers = pd.DataFrame() 
dfDataflowRelations  = pd.DataFrame() 
dfDataflowRefreshes = pd.DataFrame() 
dfDataflowDatasources = pd.DataFrame() 
dfDatamarts = pd.DataFrame() 
dfDatamartUsers = pd.DataFrame() 
dfDatasets = pd.DataFrame() 
dfDatasetUsers = pd.DataFrame() 
dfDatasetDatasources = pd.DataFrame() 
dfDatasetRefreshes = pd.DataFrame() 
dfDatasetDirectQueryRefreshes = pd.DataFrame() 
dfDatasetUpstreamDatamarts = pd.DataFrame() 
dfDatasetUpstreamDatasets = pd.DataFrame() 
dfDatasetRelations = pd.DataFrame() 
dfDatasetTables = pd.DataFrame() 
dfDatasetTableSource = pd.DataFrame() 
dfDatasetTableColumns = pd.DataFrame() 
dfDatasetTableMeasures = pd.DataFrame() 
dfDatasources = pd.DataFrame() 
dfEnvironments = pd.DataFrame() 
dfEnvironmentUsers = pd.DataFrame() 
dfEventhouses = pd.DataFrame() 
dfEventhouseUsers = pd.DataFrame() 
dfEventstreamDatasourceUsages = pd.DataFrame() 
dfEventstreamRelations = pd.DataFrame() 
dfEventstreams = pd.DataFrame() 
dfEventstreamUsers = pd.DataFrame() 
dfGraphQLApis = pd.DataFrame() 
dfGraphQLApiUsers = pd.DataFrame() 
dfKQLDashboardRelations = pd.DataFrame() 
dfKQLDashboards = pd.DataFrame() 
dfKQLDashboardUsers = pd.DataFrame() 
dfKqlDatabaseRelations = pd.DataFrame() 
dfKqlDatabases = pd.DataFrame() 
dfKqlDatabaseUsers = pd.DataFrame() 
dfKQLQuerysetRelations = pd.DataFrame() 
dfKQLQuerysets = pd.DataFrame() 
dfKQLQuerysetUsers = pd.DataFrame() 
dfLakehouses = pd.DataFrame() 
dfLakehouseUsers = pd.DataFrame() 
dfMirroredDatabases = pd.DataFrame() 
dfMirroredDatabaseUsers = pd.DataFrame() 
dfMLExperiments = pd.DataFrame() 
dfMLExperimentUsers = pd.DataFrame() 
dfMLModelRelations = pd.DataFrame() 
dfMLModels = pd.DataFrame() 
dfMLModelUsers = pd.DataFrame() 
dfMountedDataFactories = pd.DataFrame() 
dfMountedDataFactoryUsers = pd.DataFrame() 
dfNotebookRelations = pd.DataFrame() 
dfNotebooks = pd.DataFrame() 
dfNotebookUsers = pd.DataFrame() 
dfReflexes = pd.DataFrame() 
dfReflexeUsers = pd.DataFrame() 
dfReports = pd.DataFrame() 
dfReportUsers = pd.DataFrame() 
dfWarehouses = pd.DataFrame() 
dfWarehouseUsers = pd.DataFrame() 
dfWorkspaces = pd.DataFrame() 
dfWorkspaceUsers = pd.DataFrame() 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Get workspaces**

# CELL ********************

# get existing workspace ids
try:
    # if initializing
    if pInitialize == 'yes':
        vWorkspaceUrl = f"admin/workspaces/modified?excludePersonalWorkspaces=False"
    # if not use the modifiedsince parameter
    else:
        vWorkspaceUrl = f"admin/workspaces/modified?modifiedSince={vCurrentStartIso8601}&excludePersonalWorkspaces=False"

    # get the response
    workspaces_response = requests.get(vBaseUrl + vWorkspaceUrl, headers=vHeaders)
    print(workspaces_response)
    
    # json load
    workspaces_response = json.loads(workspaces_response.content)     
    
    # workspaceIds = pd.json_normalize(all_workspaces) 

    # logging
    vMessage = "processing workspace identifiers succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing workspace identifiers',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing workspace identifiers failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing workspace identifiers',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)   
    if pDebugMode == "yes":
        print(str(e))
    
    # if the extraction fails, force an exit as this step is a prerequisite
    sys.exit(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Scanner API**
# 


# CELL ********************

########################################
# iterate using a batch size
# request a scan
# get the scan status
# get the scan result
########################################

vSkip = 0
vBatch = 0
vBatchSize = 100
vBatchTimeout = 600
vInitialCountWorkspaces = len(workspaces_response)
vProcessedWorkspaces = 0
vRemainingCountWorkspaces = 0

# start timer 
start = timer()


try:

    # iterate through the batches of workspace ids
    while True:

        # set the vSkip option
        vSkip = vBatch * vBatchSize

        # calculate the remaining workspace count
        if vBatch==0: 
            vProcessedWorkspaces = vBatchSize 
        else:
            vProcessedWorkspaces = vSkip + vBatchSize  

        if vInitialCountWorkspaces - vProcessedWorkspaces >= 0:
            vRemainingCountWorkspaces = vInitialCountWorkspaces - vProcessedWorkspaces
        else:
            vRemainingCountWorkspaces = 0
             

        # generate the scan list to use as body for the post request
        workspace_array = [workspace["id"] for workspace in workspaces_response[vSkip : vSkip + vBatchSize]]
        scan_list = {"workspaces": workspace_array}

        # if the array contains workspace ids
        if workspace_array:

            # specify what should be scanned
            inventory_query = "lineage=true&datasourceDetails=true&getArtifactUsers=true"
            if pExportInventoryExpressions == 'yes':
                inventory_query += "&datasetSchema=true&datasetExpressions=true"
                # inventory_query += "&datasetSchema=true"

            # scan url and response
            scan_url = f"admin/workspaces/getInfo?{inventory_query}"
            scan_response = requests.post(vBaseUrl + scan_url, headers=vHeaders,json=scan_list)
            scan = scan_response.json()

            # logging
            vMessage = f"scan {scan['id']} started at {scan['createdDateTime']} for {len(workspace_array)} workspace(s)"
            if pDebugMode == "yes":
                print(vMessage) 

            time_remaining = vBatchTimeout

            if pThrottleScanApi == 'yes':
                time.sleep(10)

                vMessage = "throttle scan api set to true -> sleeping 10 sec"
                if pDebugMode == "yes":
                    print(vMessage)
            
            wait_for_scan_to_complete = True

            while wait_for_scan_to_complete:

                # scan status url and response
                status_url = f"admin/workspaces/scanStatus/{scan['id']}"
                status_response = requests.get(vBaseUrl + status_url, headers=vHeaders)

                # check code of response
                if status_response.status_code == 200:

                    # convert to json
                    status = status_response.json()      

                    # if scan successful
                    if status["status"] == "Succeeded":

                        vMessage = "scan succeeded"
                        if pDebugMode == "yes":
                            print(vMessage)

                        # scan result url and response
                        scan_result_url = f"admin/workspaces/scanResult/{scan['id']}"
                        scan_result_response = requests.get(vBaseUrl + scan_result_url, headers=vHeaders)
                        scan_result = json.loads(scan_result_response.content)

                        # uncomment if you need the list of scanned workspaces as a json file
                        # filepath =f"/lakehouse/default/Files/scanapi/{scan['id']}.json
                        # with open(filepath, 'w', encoding='utf-8') as f:
                        #     json.dump(scan_result, f, ensure_ascii=False, indent=4)

                        break

                    # if the scan timed out
                    if time_remaining <= 0:
                        vMessage = f"scan timed out. Last status: {status['status']}"
                        if pDebugMode == "yes":
                            print(vMessage)
                        break
                        
                else:
                    vMessage = "scan unsuccessful"
                    if pDebugMode == "yes":
                        print(vMessage)
                    break

                # sleep 10 sec
                time.sleep(10)     

                # decrement by 10 sec the remaining time
                time_remaining -= 10


            # extract the workspaces from the scan result
            try:
                # flatten workspaces
                workspaces = pd.concat([pd.json_normalize(x) for x in scan_result['workspaces']])

                # append to the workspaces dataframe
                dfWorkspaces = pd.concat([dfWorkspaces, workspaces], ignore_index=True)

            except Exception as e:
                vMessage = f"no workspaces found in scan {scan['id']}. exception: {str(e)}"
                if pDebugMode == "yes":
                    print(vMessage)

            # extract datasources from the scan result
            try:
                # flatten datasources
                datasources = pd.concat([pd.json_normalize(x, max_level=0) for x in scan_result['datasourceInstances']])

                # append to the datasources dataframe
                dfDatasources = pd.concat([dfDatasources, datasources], ignore_index=True)

            except Exception as e:
                vMessage = f"no datasources found in scan {scan['id']}. exception: {str(e)}"
                if pDebugMode == "yes":
                    print(vMessage)

            vBatch += 1
            vMessage = f"initial workspaces <{vInitialCountWorkspaces}>, remaing workspaces <{vRemainingCountWorkspaces}>, skip <{vSkip}>"
            if pDebugMode == "yes":
                print(vMessage)
        else:
            break

    # logging
    end = timer()
    vElapsedTime = timedelta(seconds=end-start)

    vMessage = "scanner api succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'scanner api',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    end = timer()
    vElapsedTime = timedelta(seconds=end-start)
    vMessage = "scanner api failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'scanner api',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
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

# **Datasources**

# CELL ********************

try:

    # add a row id
    dfDatasources['RowId'] = dfDatasources.index

    # explode the connection details column
    dfDatasourcesExploded = dfDatasources['connectionDetails'].apply(pd.Series)

    # add a row id
    dfDatasourcesExploded['RowId'] = dfDatasourcesExploded.index

    if 'path' in dfDatasourcesExploded.columns:
        
        # add the file extension if the path column is not empty 
        dfDatasourcesExploded['fileExtension'] = np.where(~(dfDatasourcesExploded['path'].isna()), dfDatasourcesExploded['path'].str.split('.').str[-1], np.nan)
    
        # add the file type description
        dfDatasourcesExploded['fileType'] = np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.csv', regex=True)), 'csv', 
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.xla', regex=True)), 'Microsoft Excel add-in or macro file',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.xlam', regex=True)), 'Microsoft Excel add-in after Excel 2007',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.xll', regex=True)), 'Microsoft Excel DLL-based add-in',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.xlm', regex=True)), 'Microsoft Excel macro before Excel 2007',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.xls', regex=True)), 'Microsoft Excel workbook before Excel 2007',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.xlsm', regex=True)), 'Microsoft Excel macro-enabled workbook after Excel 2007',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.xlsx', regex=True)), 'Microsoft Excel workbook after Excel 2007',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.xlt', regex=True)), 'Microsoft Excel template before Excel 2007',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.xltm', regex=True)), 'Microsoft Excel macro-enabled template after Excel 2007',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.xltx', regex=True)), 'Microsoft Excel template after Excel 2007',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.txt', regex=True)), 'Unformatted text file',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.csv', regex=True)), 'Comma-separated values file',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.xml', regex=True)), 'XML',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.json', regex=True)), 'JSON',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.pdf', regex=True)), 'PDF',
            np.where(~(dfDatasourcesExploded['path'].isna()) & (dfDatasourcesExploded['path'].str.contains('.parquet', regex=True)), 'Parquet','else')))))))))))))))))

    # join the datasets
    dfDatasourcesJoined = pd.merge(dfDatasources,dfDatasourcesExploded, on='RowId')

    # cast the connection details as string
    #dfDatasourcesJoined['connectionDetails'] = dfDatasourcesJoined['connectionDetails'].astype('|S')
    dfDatasourcesJoined['connectionDetails'] = dfDatasourcesJoined['connectionDetails'].str.encode('utf-8')

    # drop all NaN columns
    dfDatasourcesJoined = dfDatasourcesJoined.dropna(axis=1, how='all')
    
    # logging
    vMessage = "processing Datasources succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Datasources',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing Datasources failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Datasources',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True) 
    if pDebugMode == "yes":
        print(str(e))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reflex**   

# CELL ********************

try:

    # flatten the Reflex node
    dfReflexes = flatten_nested_json_df(dfWorkspaces[['id', 'Reflex']].explode('Reflex').dropna())

    # process the users
    if 'Reflex.users' in dfReflexes.columns:
        dfReflexeUsers = flatten_nested_json_df(dfReflexes[['Reflex.id', 'Reflex.users']].explode('Reflex.users').dropna())
    else:
        if 'Reflex.users.graphId' in dfReflexes.columns:     
            dfReflexeUsers = dfReflexes[[
                "Reflex.id",
                "Reflex.users.artifactUserAccessRight",                   
                "Reflex.users.emailAddress",                             
                "Reflex.users.displayName",                             
                "Reflex.users.identifier",                               
                "Reflex.users.graphId",                                  
                "Reflex.users.principalType",                            
                "Reflex.users.userType"
            ]].copy().dropna()

    # remove duplicated columns
    dfReflexes = dfReflexes.drop([
        "Reflex.users.artifactUserAccessRight",                   
        "Reflex.users.emailAddress",                             
        "Reflex.users.displayName",                             
        "Reflex.users.identifier",                               
        "Reflex.users.graphId",                                  
        "Reflex.users.principalType",                            
        "Reflex.users.userType"
        ], axis=1, errors='ignore')

    # logging
    vMessage = "processing Reflexes succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Reflexes',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)


except Exception as e:
    vMessage = "processing Reflexes failed"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Reflexes',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Notebook**

# CELL ********************

try:

    # flatten the Notebook node
    dfNotebooks = flatten_nested_json_df(dfWorkspaces[['id', 'Notebook']].explode('Notebook').dropna())

    # process the users
    if 'Notebook.users' in dfNotebooks.columns:
        dfNotebookUsers = flatten_nested_json_df(dfNotebooks[['Notebook.id', 'Notebook.users']].explode('Notebook.users').dropna())

    # process the relations
    if 'Notebook.relations' in dfNotebooks.columns:
        dfNotebookRelations = flatten_nested_json_df(dfNotebooks[['Notebook.id', 'Notebook.relations']].explode('Notebook.relations').dropna())

    # logging
    vMessage = "processing Notebooks succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Notebooks',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing Notebooks failed"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Notebooks',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reports**

# CELL ********************

try:

    # flatten the reports node 
    dfReports = flatten_nested_json_df(dfWorkspaces[['id', 'reports']].explode('reports').dropna())

    # process the users
    if 'reports.users' in dfReports.columns:
        dfReportUsers = flatten_nested_json_df(dfReports[['reports.id', 'reports.users']].explode('reports.users').dropna())
    else:
        dfReportUsers = dfReports[[
            "reports.id",
            "reports.users.reportUserAccessRight",
            "reports.users.emailAddress",
            "reports.users.displayName",
            "reports.users.identifier",
            "reports.users.graphId",
            "reports.users.principalType",
            "reports.users.userType"
        ]].copy().dropna()

    # drop columns
    dfReports = dfReports.drop([
        "reports.users.reportUserAccessRight",
        "reports.users.emailAddress",
        "reports.users.displayName",
        "reports.users.identifier",
        "reports.users.graphId",
        "reports.users.principalType",
        "reports.users.userType"
        ], axis=1, errors='ignore')


    # logging
    vMessage = "processing Reports succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Reports',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing Reports failed"
    loggingRow = {
        'LoadId': pLoadId,        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Reports',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Dashboards**

# CELL ********************

try:

    # flatten the dashboards node
    dfDashboards = flatten_nested_json_df(dfWorkspaces[['id', 'dashboards']].explode('dashboards').dropna())

    # process the users
    if 'dashboards.users' in dfDashboards.columns:
        dfDashboardUsers = flatten_nested_json_df(dfDashboards[['dashboards.id', 'dashboards.users']].explode('dashboards.users').dropna())
    else:
        if 'dashboards.users.dashboardUserAccessRight' in dfDashboards.columns:
            dfDashboardUsers = dfDashboards[[
                "dashboards.id",
                "dashboards.users.dashboardUserAccessRight",
                "dashboards.users.emailAddress",
                "dashboards.users.displayName",
                "dashboards.users.identifier",
                "dashboards.users.graphId",
                "dashboards.users.principalType",
                "dashboards.users.userType"
            ]].copy().dropna()

    # remove duplicated columns
    dfDashboards = dfDashboards.drop([
        'index',
        "dashboards.users.dashboardUserAccessRight",
        "dashboards.users.emailAddress",
        "dashboards.users.displayName",
        "dashboards.users.identifier",
        "dashboards.users.graphId",
        "dashboards.users.principalType",
        "dashboards.users.userType"
        ], axis=1, errors='ignore')
        

    # logging
    vMessage = "processing Dashboards succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Dashboards',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing Dashboards failed"
    loggingRow = {
        'LoadId': pLoadId,        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Dashboards',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Datasets**

# CELL ********************

# start timer 
start = timer()

try:

    # flatten the datasets node
    dfDatasets = flatten_nested_json_df(dfWorkspaces[['id', 'datasets']].explode('datasets').dropna())

    # process the users
    if 'datasets.users' in dfDatasets.columns:
        dfDatasetUsers = flatten_nested_json_df(dfDatasets[['datasets.id', 'datasets.users']].explode('datasets.users').dropna())

    # process the datasources
    if 'datasets.datasourceUsages' in dfDatasets.columns:
        dfDatasetDatasources = flatten_nested_json_df(dfDatasets[['datasets.id', 'datasets.datasourceUsages']].explode('datasets.datasourceUsages').dropna())   

    if 'datasets.refreshSchedule.times' in dfDatasets.columns:
        dfDatasetRefreshes = dfDatasets[[
            "datasets.id",
            "datasets.refreshSchedule.days",
            "datasets.refreshSchedule.times",
            "datasets.refreshSchedule.enabled",
            "datasets.refreshSchedule.localTimeZoneId",
            "datasets.refreshSchedule.notifyOption"
        ]].copy().dropna()

    if 'datasets.directQueryRefreshSchedule.times' in dfDatasets.columns:
        dfDatasetDirectQueryRefreshes = dfDatasets[[
            "datasets.id",
            "datasets.directQueryRefreshSchedule.frequency",
            "datasets.directQueryRefreshSchedule.days",
            "datasets.directQueryRefreshSchedule.times",
            "datasets.directQueryRefreshSchedule.localTimeZoneId"
        ]].copy().dropna()

    # process the upstreamDatamarts
    if 'datasets.upstreamDatamarts' in dfDatasets.columns:
        dfDatasetUpstreamDatamarts = flatten_nested_json_df(dfDatasets[['datasets.id', 'datasets.upstreamDatamarts']].explode('datasets.upstreamDatamarts').dropna())   

    # process the upstreamDatasets
    if 'datasets.upstreamDatasets' in dfDatasets.columns:
        dfDatasetUpstreamDatasets = flatten_nested_json_df(dfDatasets[['datasets.id', 'datasets.upstreamDatasets']].explode('datasets.upstreamDatasets').dropna())   
                 
    # process the relations
    if 'datasets.relations' in dfDatasets.columns:
        dfDatasetRelations = flatten_nested_json_df(dfDatasets[['datasets.id', 'datasets.relations']].explode('datasets.relations').dropna())   

    # process the tables
    if 'datasets.tables' in dfDatasets.columns:
        
        # process tables, source, columns and measures
        dfDatasetTables = flatten_nested_json_df(dfDatasets[['datasets.id', 'datasets.tables']].explode('datasets.tables').dropna())   
        dfDatasetTables['tableId'] = dfDatasetTables['datasets.id'] + '-' + dfDatasetTables['datasets.tables.name']
        dfDatasetTableSource = flatten_nested_json_df(dfDatasetTables[['tableId', 'datasets.tables.source']].explode('datasets.tables.source').dropna())
        dfDatasetTableColumns = flatten_nested_json_df(dfDatasetTables[['tableId', 'datasets.tables.columns']].explode('datasets.tables.columns').dropna())
        dfDatasetTableMeasures = flatten_nested_json_df(dfDatasetTables[['tableId', 'datasets.tables.measures']].explode('datasets.tables.measures').dropna())
        
        # remove duplicated columns
        dfDatasetTables = dfDatasetTables.drop([
            "datasets.tables.columns",
            "datasets.tables.measures"
            ], axis=1, errors='ignore')


    # remove duplicated columns
    dfDatasets = dfDatasets.drop([
        "datasets.refreshSchedule.days",
        "datasets.refreshSchedule.times",
        "datasets.refreshSchedule.enabled",
        "datasets.refreshSchedule.localTimeZoneId",
        "datasets.refreshSchedule.notifyOption"
        "datasets.directQueryRefreshSchedule.frequency",
        "datasets.directQueryRefreshSchedule.days",
        "datasets.directQueryRefreshSchedule.times",
        "datasets.directQueryRefreshSchedule.localTimeZoneId",
        "datasets.tables"
        ], axis=1, errors='ignore')

    # logging
    vMessage = "processing datasets succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing datasets',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing datasets failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing datasets',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Dataflows**

# CELL ********************

# start timer 
start = timer()

try:

    # flatten the dataflows node
    dfDataflows = flatten_nested_json_df(dfWorkspaces[['id', 'dataflows']].explode('dataflows').dropna())

    # process the users
    if 'dataflows.users' in dfDataflows.columns:
        dfDataflowUsers = flatten_nested_json_df(dfDataflows[['dataflows.objectId', 'dataflows.users']].explode('dataflows.users').dropna())

    # process the relations
    if 'dataflows.relations' in dfDataflows.columns:
        dfDataflowRelations = flatten_nested_json_df(dfDataflows[['dataflows.objectId', 'dataflows.relations']].explode('dataflows.relations').dropna())

    # process the schedule
    if 'dataflows.refreshSchedule.times' in dfDataflows.columns:
        dfDataflowRefreshes = dfDataflows[[
            "dataflows.objectId",
            "dataflows.refreshSchedule.days",
            "dataflows.refreshSchedule.times",
            "dataflows.refreshSchedule.enabled",
            "dataflows.refreshSchedule.localTimeZoneId",
            "dataflows.refreshSchedule.notifyOption"
        ]].copy().dropna()

    # process the datasources
    if 'dataflows.datasourceUsages' in dfDataflows.columns:
        dfDataflowDatasources = flatten_nested_json_df(dfDataflows[['dataflows.objectId', 'dataflows.datasourceUsages']].explode('dataflows.datasourceUsages').dropna())   

    # remove duplicated columns
    dfDataflows = dfDataflows.drop([
        "dataflows.users",
        "dataflows.refreshSchedule.days",
        "dataflows.refreshSchedule.times",
        "dataflows.refreshSchedule.enabled",
        "dataflows.refreshSchedule.localTimeZoneId",
        "dataflows.refreshSchedule.notifyOption",
        "dataflows.relations",
        "dataflows.datasourceUsages"
        ], axis=1, errors='ignore')


    # logging
    vMessage = "processing dataflows succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing dataflows',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing dataflows failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing dataflows',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Datamarts**

# CELL ********************

try:
    
    # flatten the datamarts node
    dfDatamarts = flatten_nested_json_df(dfWorkspaces[['id', 'datamarts']].explode('datamarts').dropna())

    # process the users
    if 'datamarts.users' in dfDatamarts.columns:
        dfDatamartUsers = flatten_nested_json_df(dfDatamarts[['datamarts.id', 'datamarts.users']].explode('datamarts.users').dropna())
    else:
        dfDatamartUsers = dfDatamarts[[
            "datamarts.id",
            "datamarts.users.datamartUserAccessRight",
            "datamarts.users.emailAddress",
            "datamarts.users.displayName",
            "datamarts.users.identifier",
            "datamarts.users.graphId",
            "datamarts.users.principalType",
            "datamarts.users.userType"
        ]].copy().dropna()


    # remove duplicated columns
    dfDatamarts = dfDatamarts.drop([
        "datamarts.users.datamartUserAccessRight",
        "datamarts.users.emailAddress",
        "datamarts.users.displayName",
        "datamarts.users.identifier",
        "datamarts.users.graphId",
        "datamarts.users.principalType",
        "datamarts.users.userType"
        ], axis=1, errors='ignore')


    # logging
    vMessage = "processing datamarts succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing datamarts',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)


except Exception as e:
    vMessage = "processing datamarts failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing datamarts',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Users**

# CELL ********************

try:
   
    # flatten the users node
    dfWorkspaceUsers = flatten_nested_json_df(dfWorkspaces[['id', 'users']].explode('users').dropna())

    # drop duplicates from dfWorkspaceUsers
    dfWorkspaceUsers.drop_duplicates(subset=None, keep="first", inplace=True)

    # logging
    vMessage = "flattening workspaces users succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Reflexes',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "flattening workspaces users failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Reflexes',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Lakehouse**

# CELL ********************

try:

    # flatten the Lakehouse node
    dfLakehouses = flatten_nested_json_df(dfWorkspaces[['id', 'Lakehouse']].explode('Lakehouse').dropna())

    # process the users
    if 'Lakehouse.users' in dfLakehouses.columns:
        dfLakehouseUsers = flatten_nested_json_df(dfLakehouses[['Lakehouse.id', 'Lakehouse.users']].explode('Lakehouse.users').dropna())


    # logging
    vMessage = "flattening lakehouses succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing lakehouses',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "flattening datasets failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing lakehouses',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **DataPipeline**

# CELL ********************

try:

    # flatten the DataPipeline node
    dfDataFactoryPipelines = flatten_nested_json_df(dfWorkspaces[['id', 'DataPipeline']].explode('DataPipeline').dropna())

    # process the users
    if 'DataPipeline.users' in dfDataFactoryPipelines.columns:
        dfDataFactoryPipelineUsers = flatten_nested_json_df(dfDataFactoryPipelines[['DataPipeline.id', 'DataPipeline.users']].explode('DataPipeline.users').dropna())

    # process the relations
    if 'DataPipeline.relations' in dfDataFactoryPipelines.columns:
        dfDataFactoryPipelineRelations = flatten_nested_json_df(dfDataFactoryPipelines[['DataPipeline.id', 'DataPipeline.relations']].explode('DataPipeline.relations').dropna())

    # logging
    vMessage = "processing DataPipelines succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing DataPipelines',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing DataPipelines failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing DataPipelines',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Warehouses**

# CELL ********************

try:

    # flatten the warehouses node
    dfWarehouses = flatten_nested_json_df(dfWorkspaces[['id', 'warehouses']].explode('warehouses').dropna())

    # process the users
    if 'warehouses.users' in dfWarehouses.columns:
        dfWarehouseUsers = flatten_nested_json_df(dfWarehouses[['warehouses.id', 'warehouses.users']].explode('warehouses.users').dropna())


    # logging
    vMessage = "flattening warehouses succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing warehouses',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "flattening warehouses failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing warehouses',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **GraphQLApi**

# CELL ********************

try:

    # flatten the GraphQLApi node
    dfGraphQLApis  = flatten_nested_json_df(dfWorkspaces[['id', 'GraphQLApi']].explode('GraphQLApi').dropna())

    # process the users
    if 'GraphQLApi.users' in dfGraphQLApis.columns:
        dfGraphQLApiUsers = flatten_nested_json_df(dfGraphQLApis[['GraphQLApi.id', 'GraphQLApi.users']].explode('GraphQLApi.users').dropna())
    else:
        if 'GraphQLApi.users.graphId' in dfGraphQLApis.columns:     
            dfGraphQLApiUsers = dfGraphQLApis[[
                "GraphQLApi.id",
                "GraphQLApi.users.artifactUserAccessRight",                   
                "GraphQLApi.users.emailAddress",                             
                "GraphQLApi.users.displayName",                             
                "GraphQLApi.users.identifier",                               
                "GraphQLApi.users.graphId",                                  
                "GraphQLApi.users.principalType",                            
                "GraphQLApi.users.userType"
            ]].copy().dropna()


    # remove duplicated columns
    dfGraphQLApis = dfGraphQLApis.drop([
        "GraphQLApi.users.artifactUserAccessRight",                   
        "GraphQLApi.users.emailAddress",                             
        "GraphQLApi.users.displayName",                             
        "GraphQLApi.users.identifier",                               
        "GraphQLApi.users.graphId",                                  
        "GraphQLApi.users.principalType",                            
        "GraphQLApi.users.userType"
        ], axis=1, errors='ignore')

    # logging
    vMessage = "processing GraphQLApis succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing GraphQLApis',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing GraphQLApis failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing GraphQLApis',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **DataAgent (former AISkill)**

# CELL ********************

try:

    # flatten the AISkill node
    dfDataAgents = flatten_nested_json_df(dfWorkspaces[['id', 'AISkill']].explode('AISkill').dropna())

    # process the users
    if 'AISkill.users' in dfDataAgents.columns:
        dfDataAgentUsers = flatten_nested_json_df(dfDataAgents[['AISkill.id', 'AISkill.users']].explode('AISkill.users').dropna())
    else:
        if 'AISkill.users.graphId' in dfDataAgents.columns:     
            dfDataAgentUsers = dfDataAgents[[
                "AISkill.id",
                "AISkill.users.artifactUserAccessRight",                   
                "AISkill.users.emailAddress",                             
                "AISkill.users.displayName",                             
                "AISkill.users.identifier",                               
                "AISkill.users.graphId",                                  
                "AISkill.users.principalType",                            
                "AISkill.users.userType"
            ]].copy().dropna()


    # remove duplicated columns
    dfDataAgents = dfDataAgents.drop([
        "AISkill.users.artifactUserAccessRight",                   
        "AISkill.users.emailAddress",                             
        "AISkill.users.displayName",                             
        "AISkill.users.identifier",                               
        "AISkill.users.graphId",                                  
        "AISkill.users.principalType",                            
        "AISkill.users.userType"
        ], axis=1, errors='ignore')

    # logging
    vMessage = "processing DataAgents succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing DataAgents',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True) 

except Exception as e:
    vMessage = "processing DataAgents failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing DataAgents',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **MLExperiment**                   

# CELL ********************

try:

    # flatten the MLExperiment node
    dfMLExperiments = flatten_nested_json_df(dfWorkspaces[['id', 'MLExperiment']].explode('MLExperiment').dropna())

    # process the users
    if 'MLExperiment.users' in dfMLExperiments.columns:
        dfMLExperimentUsers = flatten_nested_json_df(dfMLExperiments[['MLExperiment.id', 'MLExperiment.users']].explode('MLExperiment.users').dropna())
    else:
        if 'MLExperiment.users.graphId' in dfMLExperiments.columns:     
            dfMLExperimentUsers = dfMLExperiments[[
                "MLExperiment.id",
                "MLExperiment.users.artifactUserAccessRight",                   
                "MLExperiment.users.emailAddress",                             
                "MLExperiment.users.displayName",                             
                "MLExperiment.users.identifier",                               
                "MLExperiment.users.graphId",                                  
                "MLExperiment.users.principalType",                            
                "MLExperiment.users.userType"
            ]].copy().dropna()


    # remove duplicated columns
    dfMLExperiments = dfMLExperiments.drop([
        "MLExperiment.users.artifactUserAccessRight",                   
        "MLExperiment.users.emailAddress",                             
        "MLExperiment.users.displayName",                             
        "MLExperiment.users.identifier",                               
        "MLExperiment.users.graphId",                                  
        "MLExperiment.users.principalType",                            
        "MLExperiment.users.userType"
        ], axis=1, errors='ignore')

    # logging
    vMessage = "processing MLExperiments succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing MLExperiments',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing MLExperiments failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing MLExperiments',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **MLModel**                        

# CELL ********************

try:

    # flatten the MLModel node
    dfMLModels = flatten_nested_json_df(dfWorkspaces[['id', 'MLModel']].explode('MLModel').dropna())

    
    # process the users
    if 'MLModel.users' in dfMLModels.columns:
        dfMLModelUsers = flatten_nested_json_df(dfMLModels[['MLModel.id', 'MLModel.users']].explode('MLModel.users').dropna())
    else:
        if 'MLModel.users.graphId' in dfMLModels.columns:     
            dfMLModelUsers = dfMLModels[[
                "MLModel.id",
                "MLModel.users.artifactUserAccessRight",                   
                "MLModel.users.emailAddress",                             
                "MLModel.users.displayName",                             
                "MLModel.users.identifier",                               
                "MLModel.users.graphId",                                  
                "MLModel.users.principalType",                            
                "MLModel.users.userType"
            ]].copy().dropna()


    # process the relations
    if 'MLModel.relations.dependentOnArtifactId' in dfMLModels.columns:
        dfMLModelRelations = dfMLModels[[
            "MLModel.id",
            "MLModel.relations.dependentOnArtifactId",
            "MLModel.relations.workspaceId",
            "MLModel.relations.relationType",
            "MLModel.relations.settingsList",
            "MLModel.relations.usage"
            ]].copy().dropna()

    # remove duplicated columns
    dfMLModels = dfMLModels.drop([
        "MLModel.users.artifactUserAccessRight",                   
        "MLModel.users.emailAddress",                             
        "MLModel.users.displayName",                             
        "MLModel.users.identifier",                               
        "MLModel.users.graphId",                                  
        "MLModel.users.principalType",                            
        "MLModel.users.userType",
        "MLModel.relations.dependentOnArtifactId",
        "MLModel.relations.workspaceId",
        "MLModel.relations.relationType",
        "MLModel.relations.settingsList",
        "MLModel.relations.usage"
        ], axis=1, errors='ignore')

    # logging
    vMessage = "processing MLModels succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing MLModels',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing MLModels failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing MLModels',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **MirroredDatabase**               

# CELL ********************

try:

    # flatten the MirroredDatabase node
    dfMirroredDatabases = flatten_nested_json_df(dfWorkspaces[['id', 'MirroredDatabase']].explode('MirroredDatabase').dropna())

    # process the users
    if 'MirroredDatabase.users' in dfMirroredDatabases.columns:
        dfMirroredDatabaseUsers = flatten_nested_json_df(dfMirroredDatabases[['MirroredDatabase.id', 'MirroredDatabase.users']].explode('MirroredDatabase.users').dropna())
    else:
        if 'MirroredDatabase.users.graphId' in dfMirroredDatabases.columns:     
            dfMirroredDatabaseUsers = dfMirroredDatabases[[
                "MirroredDatabase.id",
                "MirroredDatabase.users.artifactUserAccessRight",                   
                "MirroredDatabase.users.emailAddress",                             
                "MirroredDatabase.users.displayName",                             
                "MirroredDatabase.users.identifier",                               
                "MirroredDatabase.users.graphId",                                  
                "MirroredDatabase.users.principalType",                            
                "MirroredDatabase.users.userType"
            ]].copy().dropna()


    # remove duplicated columns
    dfMirroredDatabases = dfMirroredDatabases.drop([
        "MirroredDatabase.users.artifactUserAccessRight",                   
        "MirroredDatabase.users.emailAddress",                             
        "MirroredDatabase.users.displayName",                             
        "MirroredDatabase.users.identifier",                               
        "MirroredDatabase.users.graphId",                                  
        "MirroredDatabase.users.principalType",                            
        "MirroredDatabase.users.userType"
        ], axis=1, errors='ignore')

    # logging
    vMessage = "processing MirroredDatabases succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing MirroredDatabases',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing MirroredDatabases failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing MirroredDatabases',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Environment**                    

# CELL ********************

try:

    # flatten the Environment node
    dfEnvironments = flatten_nested_json_df(dfWorkspaces[['id', 'Environment']].explode('Environment').dropna())

    # process the users
    if 'Environment.users' in dfEnvironments.columns:
        dfEnvironmentUsers = flatten_nested_json_df(dfEnvironments[['Environment.id', 'Environment.users']].explode('Environment.users').dropna())

    # logging
    vMessage = "processing Environments succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Environments',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing Environments failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Environments',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Eventhouse**

# CELL ********************

try:

    # flatten the Eventhouse node
    dfEventhouses = flatten_nested_json_df(dfWorkspaces[['id', 'Eventhouse']].explode('Eventhouse').dropna())

    # process the users
    if 'Eventhouse.users' in dfEventhouses.columns:
        dfEventhouseUsers = flatten_nested_json_df(dfEventhouses[['Eventhouse.id', 'Eventhouse.users']].explode('Eventhouse.users').dropna())
    else:
        if 'Eventhouse.users.graphId' in dfEventhouses.columns:     
            dfEventhouseUsers = dfEventhouses[[
                "Eventhouse.id",
                "Eventhouse.users.artifactUserAccessRight",                   
                "Eventhouse.users.emailAddress",                             
                "Eventhouse.users.displayName",                             
                "Eventhouse.users.identifier",                               
                "Eventhouse.users.graphId",                                  
                "Eventhouse.users.principalType",                            
                "Eventhouse.users.userType"
            ]].copy().dropna()

    # remove duplicated columns
    dfEventhouses = dfEventhouses.drop([
        "Eventhouse.users.artifactUserAccessRight",                   
        "Eventhouse.users.emailAddress",                             
        "Eventhouse.users.displayName",                             
        "Eventhouse.users.identifier",                               
        "Eventhouse.users.graphId",                                  
        "Eventhouse.users.principalType",                            
        "Eventhouse.users.userType"
        ], axis=1, errors='ignore')

    # logging
    vMessage = "processing Eventhouses succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Eventhouses',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)


except Exception as e:
    vMessage = "processing Eventhouses failed"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Eventhouses',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Eventstream**

# CELL ********************

try:

    # flatten the Eventstream node
    dfEventstreams = flatten_nested_json_df(dfWorkspaces[['id', 'Eventstream']].explode('Eventstream').dropna())

    # process the users
    if 'Eventstream.users' in dfEventstreams.columns:
        dfEventstreamUsers = flatten_nested_json_df(dfEventstreams[['Eventstream.id', 'Eventstream.users']].explode('Eventstream.users').dropna())
    else:
        if 'Eventstream.users.graphId' in dfEventstreams.columns:     
            dfEventstreamUsers = dfEventstreams[[
                "Eventstream.id",
                "Eventstream.users.artifactUserAccessRight",                   
                "Eventstream.users.emailAddress",                             
                "Eventstream.users.displayName",                             
                "Eventstream.users.identifier",                               
                "Eventstream.users.graphId",                                  
                "Eventstream.users.principalType",                            
                "Eventstream.users.userType"
            ]].copy().dropna()


    # process the relations
    if 'Eventstream.relations' in dfEventstreams.columns:
        dfEventstreamRelations = flatten_nested_json_df(dfEventstreams[['Eventstream.id', 'Eventstream.relations']].explode('Eventstream.relations').dropna())

    # process the datasource usages
    if 'Eventstream.datasourceUsages' in dfEventstreams.columns:
        dfEventstreamDatasourceUsages = flatten_nested_json_df(dfEventstreams[['Eventstream.id', 'Eventstream.datasourceUsages']].explode('Eventstream.datasourceUsages').dropna())


    # remove duplicated columns
    dfEventstreams = dfEventstreams.drop([
        "Eventstream.datasourceUsages",
        "Eventstream.relations",
        "Eventstream.users.artifactUserAccessRight",                   
        "Eventstream.users.emailAddress",                             
        "Eventstream.users.displayName",                             
        "Eventstream.users.identifier",                               
        "Eventstream.users.graphId",                                  
        "Eventstream.users.principalType",                            
        "Eventstream.users.userType"
        ], axis=1, errors='ignore')

    # logging
    vMessage = "processing Eventstreams succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Eventstreams',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing Eventstreams failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing Eventstreams',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **KQLDatabase**

# CELL ********************

try:
    # flatten the KQLDatabase node
    dfKqlDatabases = flatten_nested_json_df(dfWorkspaces[['id', 'KQLDatabase']].explode('KQLDatabase').dropna())

    # process the users
    if 'KQLDatabase.users.graphId' in dfKqlDatabases.columns:
        dfKqlDatabaseUsers = dfKqlDatabases[[
            'KQLDatabase.id',
            'KQLDatabase.users.artifactUserAccessRight',
            'KQLDatabase.users.emailAddress',
            'KQLDatabase.users.displayName',
            'KQLDatabase.users.identifier',
            'KQLDatabase.users.graphId',
            'KQLDatabase.users.principalType',
            'KQLDatabase.users.userType'
        ]].copy().dropna()

    # process the relations
    dfKqlDatabaseRelations = dfKqlDatabases[[
        'KQLDatabase.id',
        'KQLDatabase.relations.dependentOnArtifactId',
        'KQLDatabase.relations.workspaceId',
        'KQLDatabase.relations.relationType',
        'KQLDatabase.relations.settingsList',
        'KQLDatabase.relations.usage'
    ]].copy().dropna()

    # remove duplicated columns
    dfKqlDatabases = dfKqlDatabases.drop([
        'KQLDatabase.users.artifactUserAccessRight',
        'KQLDatabase.users.emailAddress',
        'KQLDatabase.users.displayName',
        'KQLDatabase.users.identifier',
        'KQLDatabase.users.graphId',
        'KQLDatabase.users.principalType',
        'KQLDatabase.users.userType',
        'KQLDatabase.relations.dependentOnArtifactId',
        'KQLDatabase.relations.workspaceId',
        'KQLDatabase.relations.relationType',
        'KQLDatabase.relations.settingsList',
        'KQLDatabase.relations.usage'
        ], axis=1, errors='ignore')


    # logging
    vMessage = "processing KQLDatabases succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing KQLDatabases',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing KQLDatabases failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing KQLDatabases',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **KQLQueryset**

# CELL ********************

try:

    # flatten the KQLQueryset node
    dfKQLQuerysets = flatten_nested_json_df(dfWorkspaces[['id', 'KQLQueryset']].explode('KQLQueryset').dropna())

    # process the users
    if 'KQLQueryset.users' in dfKQLQuerysets.columns:
        dfKQLQuerysetUsers = flatten_nested_json_df(dfKQLQuerysets[['KQLQueryset.id', 'KQLQueryset.users']].explode('KQLQueryset.users').dropna())
    else:
        if 'KQLQueryset.users.graphId' in dfKQLQuerysets.columns:     
            dfKQLQuerysetUsers = dfKQLQuerysets[[
                "KQLQueryset.id",
                "KQLQueryset.users.artifactUserAccessRight",                   
                "KQLQueryset.users.emailAddress",                             
                "KQLQueryset.users.displayName",                             
                "KQLQueryset.users.identifier",                               
                "KQLQueryset.users.graphId",                                  
                "KQLQueryset.users.principalType",                            
                "KQLQueryset.users.userType"
            ]].copy().dropna()

    dfKQLQuerysetUsers.drop_duplicates(subset=None, keep="first", inplace=True)

    # process the relations
    if 'KQLQueryset.relations.dependentOnArtifactId' in dfKQLQuerysets.columns:
        dfKQLQuerysetRelations = dfKQLQuerysets[[
            "KQLQueryset.id",
            "KQLQueryset.relations.dependentOnArtifactId",
            "KQLQueryset.relations.workspaceId",
            "KQLQueryset.relations.relationType",
            "KQLQueryset.relations.settingsList",
            "KQLQueryset.relations.usage"
            ]].copy().dropna()


    # remove duplicated columns
    dfKQLQuerysets = dfKQLQuerysets.drop([
        "KQLQueryset.users.artifactUserAccessRight",                   
        "KQLQueryset.users.emailAddress",                             
        "KQLQueryset.users.displayName",                             
        "KQLQueryset.users.identifier",                               
        "KQLQueryset.users.graphId",                                  
        "KQLQueryset.users.principalType",                            
        "KQLQueryset.users.userType",
        "KQLQueryset.relations.dependentOnArtifactId",
        "KQLQueryset.relations.workspaceId",
        "KQLQueryset.relations.relationType",
        "KQLQueryset.relations.settingsList",
        "KQLQueryset.relations.usage"
        ], axis=1, errors='ignore')


    # logging
    vMessage = "processing KQLQuerysets succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing KQLQuerysets',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing KQLQuerysets failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing KQLQuerysets',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **MountedDataFactory**

# CELL ********************

try:

    # flatten the MountedDataFactory node
    dfMountedDataFactories = flatten_nested_json_df(dfWorkspaces[['id', 'MountedDataFactory']].explode('MountedDataFactory').dropna())

    # process the users
    if 'MountedDataFactory.users' in dfMountedDataFactories.columns:
        dfMountedDataFactoryUsers = flatten_nested_json_df(dfMountedDataFactories[['MountedDataFactory.id', 'MountedDataFactory.users']].explode('MirroredDatabase.users').dropna())
    else:
        if 'MountedDataFactory.users.graphId' in dfMountedDataFactories.columns:     
            dfMountedDataFactoryUsers = dfMountedDataFactories[[
                "MountedDataFactory.id",
                "MountedDataFactory.users.artifactUserAccessRight",                   
                "MountedDataFactory.users.emailAddress",                             
                "MountedDataFactory.users.displayName",                             
                "MountedDataFactory.users.identifier",                               
                "MountedDataFactory.users.graphId",                                  
                "MountedDataFactory.users.principalType",                            
                "MountedDataFactory.users.userType"
            ]].copy().dropna()


    # remove duplicated columns
    dfMountedDataFactories = dfMountedDataFactories.drop([
        "MountedDataFactory.users.artifactUserAccessRight",                   
        "MountedDataFactory.users.emailAddress",                             
        "MountedDataFactory.users.displayName",                             
        "MountedDataFactory.users.identifier",                               
        "MountedDataFactory.users.graphId",                                  
        "MountedDataFactory.users.principalType",                            
        "MountedDataFactory.users.userType"
        ], axis=1, errors='ignore')


    # logging
    vMessage = "processing MountedDataFactories succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing MountedDataFactories',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing MountedDataFactories failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing MountedDataFactories',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **KQLDashboard**

# CELL ********************

try:

    # flatten the KQLDashboard node
    dfKQLDashboards = flatten_nested_json_df(dfWorkspaces[['id', 'KQLDashboard']].explode('KQLDashboard').dropna())

    # process the users
    if 'KQLDashboard.users' in dfKQLDashboards.columns:
        dfKQLDashboardUsers = flatten_nested_json_df(dfKQLDashboards[['KQLDashboard.id', 'KQLDashboard.users']].explode('KQLDashboard.users').dropna())
    else:
        if 'KQLDashboard.users.graphId' in dfKQLDashboards.columns:     
            dfKQLDashboardUsers = dfKQLDashboards[[
                "KQLDashboard.id",
                "KQLDashboard.users.artifactUserAccessRight",                   
                "KQLDashboard.users.emailAddress",                             
                "KQLDashboard.users.displayName",                             
                "KQLDashboard.users.identifier",                               
                "KQLDashboard.users.graphId",                                  
                "KQLDashboard.users.principalType",                            
                "KQLDashboard.users.userType"
            ]].copy().dropna()

    # process the relations
    if 'KQLDashboard.relations' in dfKQLDashboards.columns:
        dfKQLDashboardRelations = flatten_nested_json_df(dfKQLDashboards[['KQLDashboard.id', 'KQLDashboard.relations']].explode('KQLDashboard.relations').dropna())

    # remove duplicated columns
    dfKQLDashboards = dfKQLDashboards.drop([
        "KQLDashboard.users.artifactUserAccessRight",                   
        "KQLDashboard.users.emailAddress",                             
        "KQLDashboard.users.displayName",                             
        "KQLDashboard.users.identifier",                               
        "KQLDashboard.users.graphId",                                  
        "KQLDashboard.users.principalType",                            
        "KQLDashboard.users.userType"
        ], axis=1, errors='ignore')

    # logging
    vMessage = "processing KQLDashboards succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing KQLDashboards',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing KQLDashboards failed"    
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing KQLDashboards',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **SQLDatabase**

# CELL ********************

try:

    # flatten the SQLDatabase node
    dfSQLDatabases = flatten_nested_json_df(dfWorkspaces[['id', 'SQLDatabase']].explode('SQLDatabase').dropna())

    # process the users
    if 'SQLDatabase.users' in dfSQLDatabases.columns:
        dfSQLDatabaseUsers = flatten_nested_json_df(dfSQLDatabases[['SQLDatabase.id', 'SQLDatabase.users']].explode('SQLDatabase.users').dropna())

    # logging
    vMessage = "processing SQLDatabases succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing SQLDatabases',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing SQLDatabases failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing SQLDatabases',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Workspaces**

# CELL ********************

try:

    # add isOrphaned
    dfWorkspaces['isOrphaned'] = np.where(pd.isnull(dfWorkspaces['users']), True, False)

    # remove duplicated columns
    dfWorkspaces = dfWorkspaces.drop([
        'Reflex',
        'Notebook',
        'reports',
        'dashboards',
        'datasets',
        'dataflows',
        'datamarts',
        'users',
        'Lakehouse',
        'DataPipeline',
        'warehouses',
        'GraphQLApi',
        'AISkill',
        'MLExperiment',
        'MLModel',
        'MirroredDatabase',
        'CopyJob',
        'Environment',
        'Eventstream',
        'KQLDatabase',
        'Eventhouse',
        'KQLQueryset',
        'MountedDataFactory',
        'KQLDashboard',
        'SQLDatabase'
        ], axis=1, errors='ignore')


    # logging
    vMessage = "processing workspaces succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing workspaces',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "processing workspaces failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'processing workspaces',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
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
        case "dfDashboards": 
            return "dashboards"
        case "dfDashboardUsers": 
            return "dashboard_users"
        case "dfDataAgents": 
            return "data_agents"
        case "dfDataAgentUsers": 
            return "data_agent_users"
        case "dfDataFactoryPipelineRelations": 
            return "data_factory_pipeline_relations"
        case "dfDataFactoryPipelines": 
            return "data_factory_pipelines"
        case "dfDataFactoryPipelineUsers": 
            return "data_factory_pipeline_users"
        case "dfDataflows": 
            return "dataflows"
        case "dfDataflowUsers": 
            return "dataflow_users"
        case "dfDataflowRelations": 
            return "dataflow_relations"
        case "dfDataflowRefreshes": 
            return "dataflow_refreshes"
        case "dfDataflowDatasources": 
            return "dataflow_Datasources"
        case "dfDatamarts": 
            return "datamarts"
        case "dfDatamartUsers": 
            return "datamart_users"
        case "dfDatasets": 
            return "datasets"
        case "dfDatasetUsers": 
            return "dataset_users"
        case "dfDatasetDatasources": 
            return "dataset_datasources"
        case "dfDatasetRefreshes": 
            return "dataset_refreshes"
        case "dfDatasetDirectQueryRefreshes": 
            return "dataset_direct_query_refreshes"
        case "dfDatasetUpstreamDatamarts": 
            return "dataset_upstream_datamarts"
        case "dfDatasetUpstreamDatasets": 
            return "dataset_upstream_datasets"
        case "dfDatasetRelations": 
            return "dataset_relations"
        case "dfDatasetTables": 
            return "dataset_tables"
        case "dfDatasetTableSource": 
            return "dataset_table_source"
        case "dfDatasetTableColumns": 
            return "dataset_table_columns"
        case "dfDatasetTableMeasures": 
            return "dataset_table_measures"
        case "dfDatasources": 
            return "datasources"
        case "dfEnvironments": 
            return "environments"
        case "dfEnvironmentUsers": 
            return "environment_users"
        case "dfEventhouses": 
            return "eventhouses"
        case "dfEventhouseUsers": 
            return "eventhouses_users"
        case "dfEventstreamDatasourceUsages": 
            return "eventstream_datasource_usages"
        case "dfEventstreamRelations": 
            return "eventstream_relations"
        case "dfEventstreams": 
            return "eventstreams"
        case "dfEventstreamUsers": 
            return "eventstream_users"
        case "dfGraphQLApis": 
            return "graphql_apis"
        case "dfGraphQLApiUsers": 
            return "graphql_api_users"
        case "dfKQLDashboardRelations": 
            return "kql_dashboard_relations"
        case "dfKQLDashboards": 
            return "kql_dashboards"
        case "dfKQLDashboardUsers": 
            return "kql_dashboard_users"
        case "dfKqlDatabaseRelations": 
            return "kql_database_Relations"
        case "dfKqlDatabases": 
            return "kql_databases"
        case "dfKqlDatabaseUsers": 
            return "kql_database_users"
        case "dfKQLQuerysetRelations": 
            return "kql_queryset_relations"
        case "dfKQLQuerysets": 
            return "kql_Querysets"
        case "dfKQLQuerysetUsers": 
            return "kql_queryset_users"
        case "dfLakehouses": 
            return "lakehouses"
        case "dfLakehouseUsers": 
            return "lakehouse_users"
        case "dfMirroredDatabases": 
            return "mirrored_databases"
        case "dfMirroredDatabaseUsers": 
            return "mirrored_database_users"
        case "dfMLExperiments": 
            return "ml_experiments"
        case "dfMLExperimentUsers": 
            return "ml_experiment_users"
        case "dfMLModelRelations": 
            return "ml_model_relations"
        case "dfMLModels": 
            return "ml_models"
        case "dfMLModelUsers": 
            return "ml_model_users"
        case "dfMountedDataFactories": 
            return "mounted_data_factories"
        case "dfMountedDataFactoryUsers": 
            return "mounted_data_factory_users"
        case "dfNotebookRelations": 
            return "notebook_relations"
        case "dfNotebooks": 
            return "notebooks"
        case "dfNotebookUsers": 
            return "notebook_users"
        case "dfReflexes": 
            return "reflexes"
        case "dfReflexeUsers": 
            return "reflexe_users"
        case "dfReports": 
            return "reports"
        case "dfReportUsers": 
            return "report_users"
        case "dfWarehouses": 
            return "warehouses"
        case "dfWarehouseUsers": 
            return "warehouse_users"
        case "dfWorkspaces": 
            return "workspaces"
        case "dfWorkspaceUsers": 
            return "workspace_users"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Dropping duplicates**

# CELL ********************

try:
    dfDashboards.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDashboardUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDataAgents.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDataAgentUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDataFactoryPipelineRelations.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDataFactoryPipelines.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDataFactoryPipelineUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDataflows.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDataflowUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDataflowRelations.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDataflowRefreshes.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDataflowDatasources.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatamarts.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatamartUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatasets.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatasetUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatasetDatasources.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatasetRefreshes.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatasetDirectQueryRefreshes.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatasetUpstreamDatamarts.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatasetUpstreamDatasets.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatasetRelations.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatasetTables.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatasetTableSource.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatasetTableColumns.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatasetTableMeasures.drop_duplicates(subset=None, keep="first", inplace=True)
    dfDatasources.drop_duplicates(subset=None, keep="first", inplace=True)
    dfEnvironments.drop_duplicates(subset=None, keep="first", inplace=True)
    dfEnvironmentUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfEventstreamDatasourceUsages.drop_duplicates(subset=None, keep="first", inplace=True)
    dfEventstreamRelations.drop_duplicates(subset=None, keep="first", inplace=True)
    dfEventstreams.drop_duplicates(subset=None, keep="first", inplace=True)
    dfEventstreamUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfGraphQLApis.drop_duplicates(subset=None, keep="first", inplace=True)
    dfGraphQLApiUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfKQLDashboardRelations.drop_duplicates(subset=None, keep="first", inplace=True)
    dfKQLDashboards.drop_duplicates(subset=None, keep="first", inplace=True)
    dfKQLDashboardUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfKqlDatabaseRelations.drop_duplicates(subset=None, keep="first", inplace=True)
    dfKqlDatabases.drop_duplicates(subset=None, keep="first", inplace=True)
    dfKqlDatabases.drop_duplicates(subset=None, keep="first", inplace=True)
    dfKqlDatabaseUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfKqlDatabaseUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfKQLQuerysetRelations.drop_duplicates(subset=None, keep="first", inplace=True)
    dfKQLQuerysets.drop_duplicates(subset=None, keep="first", inplace=True)
    dfKQLQuerysetUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfLakehouses.drop_duplicates(subset=None, keep="first", inplace=True)
    dfLakehouseUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfMirroredDatabases.drop_duplicates(subset=None, keep="first", inplace=True)
    dfMirroredDatabaseUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfMLExperiments.drop_duplicates(subset=None, keep="first", inplace=True)
    dfMLExperimentUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfMLModelRelations.drop_duplicates(subset=None, keep="first", inplace=True)
    dfMLModels.drop_duplicates(subset=None, keep="first", inplace=True)
    dfMLModelUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfMountedDataFactories.drop_duplicates(subset=None, keep="first", inplace=True)
    dfMountedDataFactoryUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfNotebookRelations.drop_duplicates(subset=None, keep="first", inplace=True)
    dfNotebooks.drop_duplicates(subset=None, keep="first", inplace=True)
    dfNotebookUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfReflexes.drop_duplicates(subset=None, keep="first", inplace=True)
    dfReflexeUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfReports.drop_duplicates(subset=None, keep="first", inplace=True)
    dfReportUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfWarehouses.drop_duplicates(subset=None, keep="first", inplace=True)
    dfWarehouseUsers.drop_duplicates(subset=None, keep="first", inplace=True)
    dfWorkspaces.drop_duplicates(subset=None, keep="first", inplace=True)
    dfWorkspaceUsers.drop_duplicates(subset=None, keep="first", inplace=True)

    # logging
    vMessage = "dropping duplicates succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'dropping duplicates',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "dropping duplicates failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'dropping duplicates',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
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

dfList = [
    dfDashboards,
    dfDashboardUsers,
    dfDataAgents,
    dfDataAgentUsers,
    dfDataFactoryPipelineRelations,
    dfDataFactoryPipelines,
    dfDataFactoryPipelineUsers,
    dfDataflows,
    dfDataflowUsers,
    dfDataflowRelations,
    dfDataflowRefreshes,
    dfDataflowDatasources,
    dfDatamarts,
    dfDatamartUsers,
    dfDatasets,
    dfDatasetUsers,
    dfDatasetDatasources,
    dfDatasetRefreshes,
    dfDatasetDirectQueryRefreshes,
    dfDatasetUpstreamDatamarts,
    dfDatasetUpstreamDatasets,
    dfDatasetRelations,
    dfDatasetTables,
    dfDatasetTableSource,
    dfDatasetTableColumns,
    dfDatasetTableMeasures,
    dfDatasources,
    dfEnvironments,
    dfEnvironmentUsers,
    dfEventstreamDatasourceUsages,
    dfEventstreamRelations,
    dfEventstreams,
    dfEventstreamUsers,
    dfGraphQLApis,
    dfGraphQLApiUsers,
    dfKQLDashboardRelations,
    dfKQLDashboards,
    dfKQLDashboardUsers,
    dfKqlDatabaseRelations,
    dfKqlDatabases,
    dfKqlDatabaseUsers,
    dfKQLQuerysetRelations,
    dfKQLQuerysets,
    dfKQLQuerysetUsers,
    dfLakehouses,
    dfLakehouseUsers,
    dfMirroredDatabases,
    dfMirroredDatabaseUsers,
    dfMLExperiments,
    dfMLExperimentUsers,
    dfMLModelRelations,
    dfMLModels,
    dfMLModelUsers,
    dfMountedDataFactories,
    dfMountedDataFactoryUsers,
    dfNotebookRelations,
    dfNotebooks,
    dfNotebookUsers,
    dfReflexes,
    dfReflexeUsers,
    dfReports,
    dfReportUsers,
    dfWarehouses,
    dfWarehouseUsers, 
    dfWorkspaces,
    dfWorkspaceUsers
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Additional processing**

# CELL ********************

try:
    # iterate over the dataframes
    for i, df in enumerate(dfList):

        vOriginalDataframeName = get_df_name(dfList[i])
        if pDebugMode == "yes":
            print(f"processing dataframe {vOriginalDataframeName}")

        # rename the workspace id
        dfList[i].rename(columns={
            "id":"workspaceId"
            },
            inplace=True
            )

        # get the column names
        columns = dfList[i].columns.values.tolist()

        # iterate over the column name and rename after capitalizin the first letter
        for columnName in columns:

            # split the column name, take the last item and upper case the first letter
            processed_column = process_column_name(columnName, '.')

            dfList[i].rename(columns={columnName: processed_column}, inplace=True)


        # add a timestamp column
        dfList[i]['ModifiedTime'] = dt.datetime.now()

        # drop all NaN columns
        dfList[i] = dfList[i].dropna(axis=1, how='all')


    # logging
    vMessage = "additional processing succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'additional processing',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "additional processing failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'additional processing',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
    if pDebugMode == "yes":
        print(str(e))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Rebuild the list of dataframes**

# CELL ********************

dfList = [
    dfDashboards,
    dfDashboardUsers,
    dfDataAgents,
    dfDataAgentUsers,
    dfDataFactoryPipelineRelations,
    dfDataFactoryPipelines,
    dfDataFactoryPipelineUsers,
    dfDataflows,
    dfDataflowUsers,
    dfDataflowRelations,
    dfDataflowRefreshes,
    dfDataflowDatasources,
    dfDatamarts,
    dfDatamartUsers,
    dfDatasets,
    dfDatasetUsers,
    dfDatasetDatasources,
    dfDatasetRefreshes,
    dfDatasetDirectQueryRefreshes,
    dfDatasetUpstreamDatamarts,
    dfDatasetUpstreamDatasets,
    dfDatasetRelations,
    dfDatasetTables,
    dfDatasetTableSource,
    dfDatasetTableColumns,
    dfDatasetTableMeasures,
    dfDatasources,
    dfEnvironments,
    dfEnvironmentUsers,
    dfEventstreamDatasourceUsages,
    dfEventstreamRelations,
    dfEventstreams,
    dfEventstreamUsers,
    dfGraphQLApis,
    dfGraphQLApiUsers,
    dfKQLDashboardRelations,
    dfKQLDashboards,
    dfKQLDashboardUsers,
    dfKqlDatabaseRelations,
    dfKqlDatabases,
    dfKqlDatabaseUsers,
    dfKQLQuerysetRelations,
    dfKQLQuerysets,
    dfKQLQuerysetUsers,
    dfLakehouses,
    dfLakehouseUsers,
    dfMirroredDatabases,
    dfMirroredDatabaseUsers,
    dfMLExperiments,
    dfMLExperimentUsers,
    dfMLModelRelations,
    dfMLModels,
    dfMLModelUsers,
    dfMountedDataFactories,
    dfMountedDataFactoryUsers,
    dfNotebookRelations,
    dfNotebooks,
    dfNotebookUsers,
    dfReflexes,
    dfReflexeUsers,
    dfReports,
    dfReportUsers,
    dfWarehouses,
    dfWarehouseUsers, 
    dfWorkspaces,
    dfWorkspaceUsers
]

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
    vMessage = "saving to lakehouse succeeded"
    loggingRow = {
        'LoadId': pLoadId,
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'saving to lakehouse',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': ''
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)

except Exception as e:
    vMessage = "saving to lakehouse failed"
    loggingRow = {
        'LoadId': pLoadId,        
        'NotebookId': vNotebookId,
        'NotebookName': vLogNotebookName,
        'WorkspaceId': vWorkspaceId,
        'Step': 'saving to lakehouse',
        'Timestamp': datetime.now(),
        'Duration': None,
        'Message': vMessage,
        'Status': str(e) 
    }
    dfLogging = pd.concat([dfLogging, pd.DataFrame([loggingRow])], ignore_index=True)
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
    
    sparkDF.write.mode("overwrite").format("delta").partitionBy("LoadId","NotebookName").option("mergeSchema", "true").saveAsTable("staging.monitoring_notebook_logging")
except Exception as e:
    vMessage = f"saving logs to the lakehouse failed. exception: {str(e)}"
    if pDebugMode == "yes":
        print(vMessage)    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Return the inventory extraction datetime**

# CELL ********************

vExtractDateTime = datetime.now() - timedelta(days=1)

# set the result
vResult = {
    "InventoryExtractionDateTime": vExtractDateTime.strftime("%Y-%m-%d 00:00:00")
}

# exit
notebookutils.notebook.exit(str(vResult))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
