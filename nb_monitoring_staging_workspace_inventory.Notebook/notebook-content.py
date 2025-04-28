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

# CELL ********************

import time
import datetime as dt
from datetime import datetime, timedelta 

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

vContext = mssparkutils.runtime.context
vNotebookId = vContext["currentNotebookId"]
vLogNotebookName = vContext["currentNotebookName"]
vWorkspaceId = vContext["currentWorkspaceId"]

vMessage = ""

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
dfDashboardsUsers = pd.DataFrame()
dfDataflows = pd.DataFrame()
dfDataflowsRefresh = pd.DataFrame()
dfDataflowsUsers = pd.DataFrame()
dfDatamarts = pd.DataFrame()
dfDatamartsUsers = pd.DataFrame()
dfDatasets = pd.DataFrame()
dfDatasetsUsers = pd.DataFrame()
dfDatasetsDatasource = pd.DataFrame()
dfDatasetsRefresh = pd.DataFrame()
dfDatasetsDirectQueryRefresh = pd.DataFrame()
dfDatasetsTable = pd.DataFrame()
dfDatasetsTableColumnsFinal = pd.DataFrame()
dfDatasetsTableMeasuresFinal = pd.DataFrame()
dfDatasetsUpstreamDatamartsTemp = pd.DataFrame()
dfDatasetsUpstreamDatamarts = pd.DataFrame()
dfDatasetsUpstreamDatasetsTemp = pd.DataFrame()
dfDatasetsUpstreamDatasets = pd.DataFrame()
dfDatasources = pd.DataFrame()
dfKqlDatabases = pd.DataFrame()
dfKqlDatabasesUsers = pd.DataFrame()
dfLakehouses = pd.DataFrame()
dfLakehousesUsers = pd.DataFrame()
dfReports = pd.DataFrame()
dfReportsUsers = pd.DataFrame()
dfWarehouses = pd.DataFrame()
dfWarehousesUsers = pd.DataFrame()
dfWorkspaces = pd.DataFrame()
dfWorkspacesUsers = pd.DataFrame()

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
        vWorkspaceUrl = f"admin/workspaces/modified?modifiedSince={vCurrentStartIso8601}&excludePersonalWorkspaces=True"

    # get the response
    workspaces_response = requests.get(vBaseUrl + vWorkspaceUrl, headers=vHeaders)

    # json load
    workspaces_response = json.loads(workspaces_response.content)     
    # workspaceIds = pd.json_normalize(all_workspaces) 

    # logging
    vMessage = "extracting workspace identifiers succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'extracting workspace identifiers', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "extracting workspace id(s) failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'extracting workspace identifiers', datetime.now(), None, vMessage, str(e) ] 
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

# **Scan API**
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

            # dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'Workspace inventory', datetime.now(), '', vMessage, ''] 

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

    vMessage = "scan api succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'scan api', datetime.now(), vElapsedTime, vMessage, ''] 

except Exception as e:
    end = timer()
    vElapsedTime = timedelta(seconds=end-start)
    vMessage = "scan api failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'scan api', datetime.now(), vElapsedTime, vMessage, str(e) ] 
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

# **Handling datasources**

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
    vMessage = "flattening datasources succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening datasources', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "flattening datasources failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening datasources', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Handling datasets**

# CELL ********************

try:

    # flatten the dataset node
    dfDatasets = flatten_nested_json_df(dfWorkspaces[['id', 'datasets']].explode('datasets').dropna())

    # handle dfDatasetsUsers
    try:
        if 'datasets.users' in dfDatasets.columns:
            dfDatasetsUsers = flatten_nested_json_df(dfDatasets[['datasets.id', 'datasets.users']].explode('datasets.users').dropna())
        else:
            if 'datasets.users.graphId' in dfDatasets.columns:     
                dfDatasetsUsers = dfDatasets[[
                    "datasets.id",
                    "datasets.users.datasetUserAccessRight",                   
                    "datasets.users.emailAddress",                             
                    "datasets.users.displayName",                             
                    "datasets.users.identifier",                               
                    "datasets.users.graphId",                                  
                    "datasets.users.principalType",                            
                    "datasets.users.userType"
                ]].copy().dropna()

        dfDatasetsUsers.drop_duplicates(subset=None, keep="first", inplace=True)
        dfDatasetsUsers['ModifiedTime'] = dt.datetime.now()


    except Exception as e:
        vMessage = f"error extracting dataset users. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)

    # handle dfDatasetsRefresh
    try:
        if 'datasets.refreshSchedule.times' in dfDatasets.columns:

            dfDatasetsRefresh = dfDatasets[[
                "datasets.id",
                "datasets.refreshSchedule.days",
                "datasets.refreshSchedule.times",
                "datasets.refreshSchedule.enabled",
                "datasets.refreshSchedule.localTimeZoneId",
                "datasets.refreshSchedule.notifyOption"
            ]].copy().dropna()

            # convert to string -> drop duplicates -> reconvert to dict
            dfDatasetsRefresh['datasets.refreshSchedule.days'] = dfDatasetsRefresh['datasets.refreshSchedule.days'].astype(str)
            dfDatasetsRefresh['datasets.refreshSchedule.times'] = dfDatasetsRefresh['datasets.refreshSchedule.times'].astype(str)
            dfDatasetsRefresh.drop_duplicates(subset=None, keep="first", inplace=True)
            dfDatasetsRefresh['datasets.refreshSchedule.days'] = dfDatasetsRefresh['datasets.refreshSchedule.days'].map(eval)
            dfDatasetsRefresh['datasets.refreshSchedule.times'] = dfDatasetsRefresh['datasets.refreshSchedule.times'].map(eval)
            dfDatasetsRefresh['ModifiedTime'] = dt.datetime.now()

    except Exception as e:
        vMessage = f"error extracting dataset refreshes. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)

    # handle dfDatasetsDirectQueryRefresh
    try:

        if 'datasets.directQueryRefreshSchedule.days' in dfDatasets.columns:
        
            dfDatasetsDirectQueryRefresh = dfDatasets[[
                "datasets.id",
                "datasets.directQueryRefreshSchedule.frequency",
                "datasets.directQueryRefreshSchedule.days",
                "datasets.directQueryRefreshSchedule.times",
                "datasets.directQueryRefreshSchedule.localTimeZoneId"
            ]].copy().dropna()

            # convert to string -> drop duplicates -> reconvert to dict
            dfDatasetsDirectQueryRefresh['datasets.directQueryRefreshSchedule.days'] = dfDatasetsDirectQueryRefresh['datasets.directQueryRefreshSchedule.days'].astype(str)
            dfDatasetsDirectQueryRefresh['datasets.directQueryRefreshSchedule.times'] = dfDatasetsDirectQueryRefresh['datasets.directQueryRefreshSchedule.times'].astype(str)
            dfDatasetsDirectQueryRefresh.drop_duplicates(subset=None, keep="first", inplace=True)
            dfDatasetsDirectQueryRefresh['datasets.directQueryRefreshSchedule.days'] = dfDatasetsDirectQueryRefresh['datasets.directQueryRefreshSchedule.days'].map(eval)
            dfDatasetsDirectQueryRefresh['datasets.directQueryRefreshSchedule.times'] = dfDatasetsDirectQueryRefresh['datasets.directQueryRefreshSchedule.times'].map(eval)
            dfDatasetsDirectQueryRefresh['ModifiedTime'] = dt.datetime.now()

    except Exception as e:
        vMessage = f"error extracting dataset direct query refreshes. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)

    # handle dfDatasetsUpstreamDatamarts
    try:

        if 'datasets.upstreamDatamarts' in dfDatasets.columns:
        
            dfDatasetsUpstreamDatamartsTemp = dfDatasets[[
                "datasets.id",
                "datasets.upstreamDatamarts"
            ]].copy().dropna()

            dfDatasetsUpstreamDatamarts = flatten_nested_json_df(dfDatasetsUpstreamDatamartsTemp[['datasets.id', 'datasets.upstreamDatamarts']].explode('datasets.upstreamDatamarts').dropna())
            dfDatasetsUpstreamDatamarts.drop_duplicates(subset=None, keep="first", inplace=True)
            dfDatasetsUpstreamDatamarts['ModifiedTime'] = dt.datetime.now()

    except Exception as e:
        vMessage = f"error extracting upstream datamarts. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)

    # handle dfDatasetsUpstreamDatasets
    try:  

        if 'datasets.upstreamDatasets' in dfDatasets.columns:
            dfDatasetsUpstreamDatasetsTemp = dfDatasets[[
                "datasets.id",
                "datasets.upstreamDatasets"
            ]].dropna()

            dfDatasetsUpstreamDatasets = flatten_nested_json_df(dfDatasetsUpstreamDatasetsTemp[['datasets.id', 'datasets.upstreamDatasets']].explode('datasets.upstreamDatasets').dropna())
            dfDatasetsUpstreamDatasets.drop_duplicates(subset=None, keep="first", inplace=True)
            dfDatasetsUpstreamDatasets['ModifiedTime'] = dt.datetime.now()

    except Exception as e:
        vMessage = f"error extracting upstream dataset. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)

    # handle dfDatasetsDatasource 
    try: 

        if 'datasets.datasourceUsages' in dfDatasets.columns:

            dfDatasetsDatasourceTemp = dfDatasets[[
                "datasets.id",
                "datasets.datasourceUsages"
            ]].copy().dropna()

            # convert datasets.tables to string -> drop duplicates -> reconvert to dict
            dfDatasetsDatasourceTemp['datasets.datasourceUsages'] = dfDatasetsDatasourceTemp['datasets.datasourceUsages'].astype(str)
            dfDatasetsDatasourceTemp.drop_duplicates(subset=None, keep="first", inplace=True)
            dfDatasetsDatasourceTemp['datasets.datasourceUsages'] = dfDatasetsDatasourceTemp['datasets.datasourceUsages'].map(eval)
            dfDatasetsDatasource = flatten_nested_json_df(dfDatasetsDatasourceTemp[['datasets.id', 'datasets.datasourceUsages']].explode('datasets.datasourceUsages').dropna())
            dfDatasetsDatasource.rename(columns={
                "datasets.datasourceUsages.datasourceInstanceId":"dataSourceId",
                },
                inplace=True
            )
            dfDatasetsDatasource['ModifiedTime'] = dt.datetime.now()

    except Exception as e:
        vMessage = f"error extracting dataset datasources. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)

    # handle dfDatasetsTable
    try:  

        dfDatasetsTableTemp = dfDatasets[[
            "datasets.id",
            "datasets.tables"
        ]].copy().dropna()

        # convert datasets.tables to string -> drop duplicates -> reconvert to dict
        dfDatasetsTableTemp['datasets.tables'] = dfDatasetsTableTemp['datasets.tables'].astype(str)
        dfDatasetsTableTemp.drop_duplicates(subset=None, keep="first", inplace=True)
        dfDatasetsTableTemp['datasets.tables'] = dfDatasetsTableTemp['datasets.tables'].map(eval)

        # normalize the json column 
        dfDatasetsTable = pd.json_normalize(dfDatasetsTableTemp['datasets.tables']).set_index(dfDatasetsTableTemp['datasets.id'])
        
        # add the dataset.id as a column 
        dfDatasetsTable['datasets.id'] = dfDatasetsTable.index
        
        # add the RowId
        dfDatasetsTable.insert(0, 'RowId', range(0, 0 + len(dfDatasetsTable)))
        
        dfDatasetsTable['ModifiedTime'] = dt.datetime.now()

        dfDatasetsTable['tableId'] = dfDatasetsTable['datasets.id'] + '-' + dfDatasetsTable['name']

        # handle dfDatasetsTableColumns
        dfDatasetsTableColumns = flatten_nested_json_df(dfDatasetsTable[['RowId', 'columns']].explode('columns').dropna())

        # handle dfDatasetsTableMeasures
        dfDatasetsTableMeasures = flatten_nested_json_df(dfDatasetsTable[['RowId', 'measures']].explode('measures').dropna())

        # final dataframes for columns and measures
        dfDatasetsTable = dfDatasetsTable.drop([
            "index",
            "measures",
            "columns",
            # "datasets.id"
            ], axis=1, errors='ignore')

        dfDatasetsTableColumnsFinal = pd.merge(dfDatasetsTable,dfDatasetsTableColumns, on='RowId')
        dfDatasetsTableMeasuresFinal = pd.merge(dfDatasetsTable,dfDatasetsTableMeasures, on='RowId')

        dfDatasetsTableColumnsFinal = dfDatasetsTableColumnsFinal.drop([
            "datasets.id_y"
            ], axis=1, errors='ignore')

        dfDatasetsTableColumnsFinal.rename(columns={
            "datasets.id_x":"datasetsId",
            "columns.name":"columnsName",
            "columns.isHidden":"columnsIsHidden"
            },
            inplace=True
            )

        dfDatasetsTableMeasuresFinal = dfDatasetsTableMeasuresFinal.drop([
            "datasets.id_y"
            ], axis=1, errors='ignore')

        dfDatasetsTableMeasuresFinal.rename(columns={
            "datasets.id_x":"datasetsId",
            "columns.name":"columnsName",
            "columns.isHidden":"columnsIsHidden"
            },
            inplace=True
            )

        dfDatasetsTableColumnsFinal['tableId'] = dfDatasetsTableColumnsFinal['datasetsId'] + '-' + dfDatasetsTableColumnsFinal['name']
        dfDatasetsTableColumnsFinal['ModifiedTime'] = dt.datetime.now()

        dfDatasetsTableMeasuresFinal['tableId'] = dfDatasetsTableMeasuresFinal['datasetsId'] + '-' + dfDatasetsTableMeasuresFinal['name']
        dfDatasetsTableMeasuresFinal['ModifiedTime'] = dt.datetime.now()

    except Exception as e:
        vMessage = f"error extracting dataset tables or columns or measures. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)


    # rename the workspace id
    dfDatasets.rename(columns={
        "id":"workspaceId"
        },
        inplace=True
        )


    # drop additional columns from dfDatasets
    dfDatasets = dfDatasets.drop([
        # "index",
        "datasets.expressions",
        "datasets.refreshSchedule.days",
        "datasets.refreshSchedule.times",
        "datasets.refreshSchedule.enabled",
        "datasets.refreshSchedule.localTimeZoneId",
        "datasets.refreshSchedule.notifyOption",
        "datasets.relations",
        # "datasets.endorsementDetails.endorsement",
        "datasets.directQueryRefreshSchedule.frequency",
        "datasets.directQueryRefreshSchedule.days",
        "datasets.directQueryRefreshSchedule.times",
        "datasets.directQueryRefreshSchedule.localTimeZoneId",
        "datasets.users",
        "datasets.datasourceUsages",
        "datasets.tables",
        "datasets.upstreamDatamarts",
        "datasets.misconfiguredDatasourceUsages",
        "datasets.upstreamDatasets",
        ], axis=1, errors='ignore')


    # drop duplicates from dfDatasets
    dfDatasets.drop_duplicates(subset=["datasets.id",], keep="first", inplace=True)

    # add a timestamp column
    dfDatasets['ModifiedTime'] = dt.datetime.now()

    # drop all NaN columns
    dfDatasets = dfDatasets.dropna(axis=1, how='all')

    # logging
    vMessage = "flattening datasets succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening datasets', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "flattening datasets failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening datasets', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Handling lakehouses**

# CELL ********************

try:

    # flatten the Lakehouse node
    dfLakehouses = flatten_nested_json_df(dfWorkspaces[['id', 'Lakehouse']].explode('Lakehouse').dropna())

    # handle dfLakehousesUsers
    try:

        if 'Lakehouse.users' in dfLakehouses.columns:
            dfLakehousesUsers = flatten_nested_json_df(dfLakehouses[['Lakehouse.id', 'Lakehouse.users']].explode('Lakehouse.users').dropna())
        else:
            dfLakehousesUsers = dfLakehouses[[
                "Lakehouse.id",
                "Lakehouse.users.artifactUserAccessRight",         
                "Lakehouse.users.emailAddress",          
                "Lakehouse.users.displayName",                       
                "Lakehouse.users.identifier",                        
                "Lakehouse.users.graphId",                           
                "Lakehouse.users.principalType",                     
                "Lakehouse.users.userType"                          
            ]].copy().dropna()

        dfLakehousesUsers.drop_duplicates(subset=None, keep="first", inplace=True)
        dfLakehousesUsers['ModifiedTime'] = dt.datetime.now()

    except Exception as e:
        vMessage = f"error extracting lakehouse users. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)

    # rename the workspace id
    dfLakehouses.rename(columns={
        "id":"workspaceId"
        },
        inplace=True
        )

    # drop columns
    dfLakehouses = dfLakehouses.drop([
        "Lakehouse.users",
        ], axis=1, errors='ignore')

    # add a timestamp column
    dfLakehouses['ModifiedTime'] = dt.datetime.now()

    # drop all NaN columns
    dfLakehouses = dfLakehouses.dropna(axis=1, how='all')

    # logging
    vMessage = "flattening lakehouses succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening lakehouses', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "flattening datasets failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening lakehouses', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Handling warehouses**

# CELL ********************

try:

    # flatten the warehouses node
    dfWarehouses = flatten_nested_json_df(dfWorkspaces[['id', 'warehouses']].explode('warehouses').dropna())

    # handle dfWarehousesUsers
    try:

        dfWarehousesUsers = dfWarehouses[[
            "warehouses.id",
            "warehouses.users.datamartUserAccessRight",
            "warehouses.users.emailAddress",
            "warehouses.users.displayName",
            "warehouses.users.identifier",
            "warehouses.users.graphId",
            "warehouses.users.principalType",
            "warehouses.users.userType"
        ]].copy().dropna()

        dfWarehousesUsers['ModifiedTime'] = dt.datetime.now()

        # drop columns
        dfWarehouses = dfWarehouses.drop([
            "index",
            "warehouses.users.datamartUserAccessRight",
            "warehouses.users.emailAddress",
            "warehouses.users.displayName",
            "warehouses.users.identifier",
            "warehouses.users.graphId",
            "warehouses.users.principalType",
            "warehouses.users.userType"
            ], axis=1, errors='ignore')

    except Exception as e:
        vMessage = f"error extracting warehouse users. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)

    # rename the workspace id
    dfWarehouses.rename(columns={
        "id":"workspaceId"
        },
        inplace=True
        )

    # add a timestamp column
    dfWarehouses['ModifiedTime'] = dt.datetime.now()

    # drop all NaN columns
    dfWarehouses = dfWarehouses.dropna(axis=1, how='all')

    # logging
    vMessage = "flattening warehouses succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening warehouses', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "flattening warehouses failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening warehouses', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Handle pipelines to be implemented**

# MARKDOWN ********************

# **Handle notebooks to be implemented**

# MARKDOWN ********************

# **Handle eventstreams to be implemented**

# MARKDOWN ********************

# **KQL database**

# CELL ********************

try:
    # flatten the KQLDatabase node
    dfKqlDatabases = flatten_nested_json_df(dfWorkspaces[['id', 'KQLDatabase']].explode('KQLDatabase').dropna())

    try:

        # handle dfKqlDatabasesUsers
        dfKqlDatabasesUsers = dfKqlDatabases[[
            'KQLDatabase.id',
            'KQLDatabase.users.artifactUserAccessRight',
            'KQLDatabase.users.emailAddress',
            'KQLDatabase.users.displayName',
            'KQLDatabase.users.identifier',
            'KQLDatabase.users.graphId',
            'KQLDatabase.users.principalType',
            'KQLDatabase.users.userType'
        ]].copy().dropna()

        dfKqlDatabasesUsers['ModifiedTime'] = dt.datetime.now()

        # drop columns
        dfKqlDatabases = dfKqlDatabases.drop([
            'KQLDatabase.users.artifactUserAccessRight',
            'KQLDatabase.users.emailAddress',
            'KQLDatabase.users.displayName',
            'KQLDatabase.users.identifier',
            'KQLDatabase.users.graphId',
            'KQLDatabase.users.principalType',
            'KQLDatabase.users.userType'
            ], axis=1, errors='ignore')

    except Exception as e:
        vMessage = f"error extracting KQL database users. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)


    # rename the workspace id
    dfKqlDatabases.rename(columns={
        "id":"workspaceId"
        },
        inplace=True
        )

    # add a timestamp column
    dfKqlDatabases['ModifiedTime'] = dt.datetime.now()

    # drop all NaN columns
    dfKqlDatabases = dfKqlDatabases.dropna(axis=1, how='all')
  

    # logging
    vMessage = "flattening KQL databases succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening KQL databases', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "flattening KQL databases failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening KQL databases', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Handling datamart**

# CELL ********************

try:
    
    # flatten the datamarts node
    dfDatamarts = flatten_nested_json_df(dfWorkspaces[['id', 'datamarts']].explode('datamarts').dropna())

    try:

        # handle dfDatamartsUsers
        dfDatamartsUsers = dfDatamarts[[
            "datamarts.id",
            "datamarts.users.datamartUserAccessRight",
            "datamarts.users.emailAddress",
            "datamarts.users.displayName",
            "datamarts.users.identifier",
            "datamarts.users.graphId",
            "datamarts.users.principalType",
            "datamarts.users.userType"
        ]].copy().dropna()

        dfDatamartsUsers['ModifiedTime'] = dt.datetime.now()

        # drop columns
        dfDatamarts = dfDatamarts.drop([
            "index",
            "datamarts.users.datamartUserAccessRight",
            "datamarts.users.emailAddress",
            "datamarts.users.displayName",
            "datamarts.users.identifier",
            "datamarts.users.graphId",
            "datamarts.users.principalType",	
            "datamarts.users.userType",
            ], axis=1, errors='ignore')

    except Exception as e:
        vMessage = f"error extracting datamart users. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)

    # rename the workspace id
    dfDatamarts.rename(columns={
        "id":"workspaceId"
        },
        inplace=True
        )

    # add a timestamp column
    dfDatamarts['ModifiedTime'] = dt.datetime.now()

    # drop all NaN columns
    dfDatamarts = dfDatamarts.dropna(axis=1, how='all')

    # logging
    vMessage = "flattening datamarts succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening datamarts', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "flattening datamarts failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening datamarts', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Handling workspace users**

# CELL ********************

try:
   
    # flatten the users node
    dfWorkspacesUsers = flatten_nested_json_df(dfWorkspaces[['id', 'users']].explode('users').dropna())

    # drop duplicates from dfWorkspacesUsers
    dfWorkspacesUsers.drop_duplicates(subset=None, keep="first", inplace=True)

    # rename the workspace id
    dfWorkspacesUsers.rename(columns={
        "id":"workspaceId"
        },
        inplace=True
        )

    # add a timestamp column
    dfWorkspacesUsers['ModifiedTime'] = dt.datetime.now()

    # drop all NaN columns
    dfWorkspacesUsers = dfWorkspacesUsers.dropna(axis=1, how='all')

    # logging
    vMessage = "flattening workspaces users succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening workspaces users', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "flattening workspaces users failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening workspaces users', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Handling reports**

# CELL ********************

try:

    # flatten the reports node 
    dfReports = flatten_nested_json_df(dfWorkspaces[['id', 'reports']].explode('reports').dropna())

    # handle dfKqlDatabasesUsers
    try:
        
        if 'reports.users' in dfReports.columns:
            
            dfReportsUsers = flatten_nested_json_df(dfReports[['reports.id', 'reports.users']].explode('reports.users').dropna())
        else:
            dfReportsUsers = dfReports[[
                "reports.id",
                "reports.users.reportUserAccessRight",
                "reports.users.emailAddress",
                "reports.users.displayName",
                "reports.users.identifier",
                "reports.users.graphId",
                "reports.users.principalType",
                "reports.users.userType"
            ]].copy().dropna()


        dfReportsUsers.drop_duplicates(subset=None, keep="first", inplace=True)
        dfReportsUsers['ModifiedTime'] = dt.datetime.now()

        # drop columns
        dfReports = dfReports.drop([
            "reports.users",
            ], axis=1, errors='ignore')

    except Exception as e:
        vMessage = f"error extracting report users. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)

    # rename the workspace id
    dfReports.rename(columns={
        "id":"workspaceId"
        },
        inplace=True
        )

    # add a timestamp column
    dfReports['ModifiedTime'] = dt.datetime.now()

    # drop all NaN columns
    dfReports = dfReports.dropna(axis=1, how='all')


    # logging
    vMessage = "flattening reports succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening reports', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "flattening reports failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening reports', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Handling dashboards**

# CELL ********************

try:

    # flatten the dashboards node
    dfDashboards = flatten_nested_json_df(dfWorkspaces[['id', 'dashboards']].explode('dashboards').dropna())

    try:

        # handle dashboard users
        if 'dashboards.users' in dfDashboards.columns:
            dfDashboardsUsers = flatten_nested_json_df(dfDashboards[['dashboards.id', 'dashboards.users']].explode('dashboards.users').dropna())
        else:

            if 'dashboards.users.dashboardUserAccessRight' in dfDashboards.columns:

                # handle dfDashboardsUsers
                dfDashboardsUsers = dfDashboards[[
                    "dashboards.id",
                    "dashboards.users.dashboardUserAccessRight",
                    "dashboards.users.emailAddress",
                    "dashboards.users.displayName",
                    "dashboards.users.identifier",
                    "dashboards.users.graphId",
                    "dashboards.users.principalType",
                    "dashboards.users.userType"
                ]].copy().dropna()

                # drop additional columns 
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
            
        # add a modified datetime
        dfDashboardsUsers['ModifiedTime'] = dt.datetime.now()

    except Exception as e:
        vMessage = f"error extracting dashboards users. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)

    # rename the workspace id
    dfDashboards.rename(columns={
        "id":"workspaceId"
        },
        inplace=True
        )

    # add embed url
    dfDashboards["embedUrl"] = ''

    # add a timestamp column
    dfDashboards['ModifiedTime'] = dt.datetime.now()

    # drop all NaN columns
    dfDashboards = dfDashboards.dropna(axis=1, how='all')


    # logging
    vMessage = "flattening dashboards succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening dashboards', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "flattening dashboards failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening dashboards', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Handling dataflows**

# CELL ********************

# start timer 
start = timer()

try:

    # flatten the dataflows node
    dfDataflows = flatten_nested_json_df(dfWorkspaces[['id', 'dataflows']].explode('dataflows').dropna())

    try:

        if 'dataflows.users' in dfDataflows.columns:
            dfDataflowsUsers = flatten_nested_json_df(dfDataflows[['dataflows.objectId', 'dataflows.users']].explode('dataflows.users').dropna())
        else:
            dfDataflowsUsers = dfDataflows[[
                "dataflows.objectId",
                "dataflows.users.dataflowUserAccessRight",
                "dataflows.users.emailAddress",
                "dataflows.users.displayName",
                "dataflows.users.identifier",
                "dataflows.users.graphId",
                "dataflows.users.principalType",
                "dataflows.users.userType"
            ]].copy().dropna()

        if 'dataflows.refreshSchedule.days' in dfDataflows.columns:
            dfDataflowsRefresh = dfDataflows[[
                "dataflows.objectId",
                "dataflows.refreshSchedule.days",
                "dataflows.refreshSchedule.times",
                "dataflows.refreshSchedule.enabled",
                "dataflows.refreshSchedule.localTimeZoneId",
                "dataflows.refreshSchedule.notifyOption"
            ]].copy().dropna()


        # drop columns 
        dfDataflows = dfDataflows.drop([
            "index",
            "dataflows.refreshSchedule.days",
            "dataflows.refreshSchedule.times",
            "dataflows.refreshSchedule.enabled",
            "dataflows.refreshSchedule.localTimeZoneId",
            "dataflows.refreshSchedule.notifyOption",
            "dataflows.users.dataflowUserAccessRight",
            "dataflows.users.emailAddress",
            "dataflows.users.displayName",
            "dataflows.users.identifier",
            "dataflows.users.graphId",
            "dataflows.users.principalType",
            "dataflows.users.userType",
            ], axis=1, errors='ignore')

        # additional columns
        dfDataflows["modelUrl"] = ''
        dfDataflowsUsers['ModifiedTime'] = dt.datetime.now()
        dfDataflowsRefresh['ModifiedTime'] = dt.datetime.now()

    except Exception as e:
        vMessage = f"error extracting dataflows users. exception: {str(e)}"
        if pDebugMode == "yes":
            print(vMessage)

    # rename the workspace id
    dfDataflows.rename(columns={
        "id":"workspaceId"
        },
        inplace=True
        )

    # additional columns
    dfDataflows['ModifiedTime'] = dt.datetime.now()

    # drop all NaN columns
    dfDataflows = dfDataflows.dropna(axis=1, how='all')

    # logging
    vMessage = "flattening dataflows succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening dataflows', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "flattening dataflows failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening dataflows', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Handling workspaces**

# CELL ********************

import numpy as np

try:

    # add isOrphaned
    dfWorkspaces['isOrphaned'] = np.where(pd.isnull(dfWorkspaces['users']), True, False)

    # drop columns 
    dfWorkspaces = dfWorkspaces.drop([
        "reports",
        "dashboards",
        "datasets",
        "dataflows",
        "datamarts",
        "users",
        "Lakehouse",
        "DataPipeline",
        "Notebook",
        "warehouses",
        "SQLEndpoints",
        "Eventstream",
        "KQLDatabase",
        "kustoeventhubdataconnection"
        ], axis=1, errors='ignore')

    # rename the workspace id
    dfWorkspaces.rename(columns={
        "id":"workspaceId"
        },
        inplace=True
        )

    # add a timestamp column
    dfWorkspaces['ModifiedTime'] = dt.datetime.now()

    # drop all NaN columns
    dfWorkspaces = dfWorkspaces.dropna(axis=1, how='all')


    # logging
    vMessage = "flattening workspaces succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening workspaces', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "flattening workspaces failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'flattening workspaces', datetime.now(), None, vMessage, str(e) ] 
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
    dfDashboards,
    dfDashboardsUsers,
    dfDataflows,
    dfDataflowsRefresh,
    dfDataflowsUsers,
    dfDatamarts,
    dfDatamartsUsers,
    dfDatasets,
    dfDatasetsUsers,
    dfDatasetsDatasource,
    dfDatasetsRefresh,
    dfDatasetsDirectQueryRefresh,
    dfDatasetsTable,
    dfDatasetsTableColumnsFinal,
    dfDatasetsTableMeasuresFinal,
    dfDatasetsUpstreamDatamarts,
    dfDatasetsUpstreamDatasets,
    dfDatasourcesJoined,
    dfKqlDatabases,
    dfKqlDatabasesUsers,
    dfLakehouses,
    dfLakehousesUsers,
    dfReports,
    dfReportsUsers,
    dfWarehouses,
    dfWarehousesUsers,
    dfWorkspaces,
    dfWorkspacesUsers
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

            # # replace the column name in the dataframe
            # dfList[i].columns = dfList[i].columns.str.replace(columnName, processed_column, regex=True)

            dfList[i].rename(columns={columnName: processed_column}, inplace=True)

        # # convert all columns to string
        # all_columns = list(dfList[i]) # Creates list of all column headers
        # dfList[i][all_columns] = dfList[i][all_columns].astype(str)


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

# **Removing duplicates**

# CELL ********************

try:
    for i, df in enumerate(dfList):
        dfList[i].drop_duplicates(subset=None, keep="first", inplace=True)

    # logging
    vMessage = "removing duplicates from dataframes succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'removing duplicates from dataframes', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = "removing duplicates from dataframes failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'removing duplicates from dataframes', datetime.now(), None, vMessage, str(e) ] 
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
        case "dfDashboardsUsers":
            return "dashboard_users"
        case "dfDataflows":
            return "dataflows"
        case "dfDataflowsRefresh":
            return "dataflow_refresh"
        case "dfDataflowsUsers":
            return "dataflow_users"
        case "dfDatamarts":
            return "datamarts"
        case "dfDatamartsUsers":
            return "datamart_users"
        case "dfDatasets":
            return "datasets"
        case "dfDatasetsUsers":
            return "dataset_users"
        case "dfDatasetsDatasource":
            return "dataset_datasources"
        case "dfDatasetsRefresh":
            return "dataset_refreshes"
        case "dfDatasetsDirectQueryRefresh":
            return "dataset_directquery_refreshes"
        case "dfDatasetsTable":
            return "dataset_tables"
        case "dfDatasetsTableColumnsFinal":
            return "dataset_table_columns"
        case "dfDatasetsTableMeasuresFinal":
            return "dataset_table_measures"
        case "dfDatasetsUpstreamDatamarts":
            return "dataset_upstream_datamarts"
        case "dfDatasetsUpstreamDatasets":
            return "dataset_upstream_datasets"
        case "dfDatasourcesJoined":
            return "datasources"
        case "dfKqlDatabases":
            return "kql_databases"
        case "dfKqlDatabasesUsers":
            return "kql_database_users"
        case "dfLakehouses":
            return "lakehouses"
        case "dfLakehousesUsers":
            return "lakehouse_users"
        case "dfReports":
            return "reports"
        case "dfReportsUsers":
            return "report_users"
        case "dfWarehouses":
            return "warehouses"
        case "dfWarehousesUsers":
            return "warehouse_users"
        case "dfWorkspaces":
            return "workspaces"
        case "dfWorkspacesUsers":
            return "workspace_users"


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
