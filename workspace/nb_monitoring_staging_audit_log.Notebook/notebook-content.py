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

# CELL ********************

pLoadId = "1"
pToken = ""
pDebugMode = "yes"
pAuditLogTimeframeInMinutes = "660"
pAllActivities = 'yes'
pInitialization =   'no'
pDateAndTime = '2025-05-20 00:00:00'

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

vApiVersion = "v1.0"
vBaseUrl = f'https://api.powerbi.com/{vApiVersion}/myorg/'
vHeader = {'Authorization': f'Bearer {vAccessToken}'} 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Functions**

# CELL ********************

def extract_audit_logs(url, header, df, audit_date):

    # make the audit log dataframe global
    global df_audit_log

    # set boolean value to continue to the next interval (in case response has a paging url)
    vContinueToNextInterval = True

    # while loop the boolean is true
    while vContinueToNextInterval:

        try:
            response = client.get(url, headers=header)

            if response.status_code != 200:
                raise FabricHTTPException(response)

        except FabricHTTPException as e:
            print("extracting audit logs failed. exception:", str(e))
              

        # response data
        response_data = response.json()


        # check if there are audit logs in response
        if len(response_data['activityEventEntities']) == 0:
            vMessage = f"0 audit logs for {audit_date} and url {url}"
            if pDebugMode == "yes":
                print(vMessage) 
            break
        
        # if no response exit while loop
        if response_data is None:
            vContinueToNextInterval = False
            vMessage = f"response is None for {audit_date} and url {url}"
            if pDebugMode == "yes":
                print(vMessage)
            break


        # count activiy logs 
        current_count = len(response_data['activityEventEntities'])
        vMessage = f"extracted {current_count} audit logs for {audit_date} and url {url}"
        if pDebugMode == "yes":
            print(vMessage)

        # load response as json
        response = json.loads(response.content)

        # flatten activityEventEntities
        audit_log = pd.concat([pd.json_normalize(x) for x in response['activityEventEntities']])

        # concat with df_audit_log if audit_log is not empty
        if not audit_log.empty:
            df_audit_log = pd.concat([df_audit_log, audit_log], ignore_index=True)


        # extract the contination token
        vContinuationToken = response_data.get('continuationToken', None)

        vContinuationCount = 0

        # while there is a continuation token, extract the next activity logs 
        while vContinuationToken:

            vContinuationCount += 1

            # next uri 
            next_logs_url = response_data['continuationUri']

            # next response          
            try:   
                response = client.get(next_logs_url, headers=header)
                if response.status_code != 200:
                    raise FabricHTTPException(response)

            except FabricHTTPException as e:
                print("extracting audit logs failed. exception:", str(e))

            # response data
            response_data = response.json()

            # check if there are audit logs in response
            if len(response_data['activityEventEntities']) == 0:
                vMessage = f"0 audit logs for {audit_date} and url {next_logs_url}"
                if pDebugMode == "yes":
                    print(vMessage)
                break

            if response_data is None:
                vContinueToNextInterval = False
                vMessage = f"response is None for {audit_date} and url {next_logs_url}"
                if pDebugMode == "yes":
                    print(vMessage)
                break


            # count activiy logs 
            current_count = len(response_data['activityEventEntities'])
            vMessage = f"extracted {current_count} audit logs for {audit_date} and url {next_logs_url}"
            if pDebugMode == "yes":
                print(vMessage)

            # load response as json
            response = json.loads(response.content)

            # flatten activityEventEntities
            audit_log = pd.concat([pd.json_normalize(x) for x in response['activityEventEntities']])

            # concat with df_audit_log if audit_log is not empty
            if not audit_log.empty:
                df_audit_log = pd.concat([df_audit_log, audit_log], ignore_index=True)

            # get the continuation token
            vContinuationToken = response_data.get('continuationToken', None)
            if pDebugMode == "yes":
                print(vContinuationToken)
        
        # if no error exit the while loop
        vContinueToNextInterval = False   


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Audit log dataframe**

# CELL ********************

df_audit_log = pd.DataFrame()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Extraction**

# CELL ********************

vDateFormat = '%Y-%m-%d %H:%M:%S'

if pInitialization == "yes":

    vRange = 27
    # create a range of dates, starting from the previous date
    vEndDate = datetime.today() - timedelta(days=1)
    vListOfDates = [
        (vEndDate - timedelta(days=i)).strftime('%Y-%m-%d 00:00:00')
        for i in range(vRange)
    ]
    vListOfDates.reverse()
    df_audit_dates = pd.DataFrame({'audit_date': vListOfDates})

    # iterate over the dates and extract the audit log
    for audit_date in df_audit_dates['audit_date']:

        # prepare the start and end date for the extraction
        vCurrentStart = datetime.strptime(audit_date,vDateFormat)
        vCurrentEnd = vCurrentStart + timedelta(hours= 23, minutes=59, seconds=59) 
        vCurrentStartIso8601 = vCurrentStart.strftime('%Y-%m-%dT%H:%M:%S.')+'000Z'
        vCurrentEndIso8601 = vCurrentEnd.strftime('%Y-%m-%dT%H:%M:%S.')+'000Z'       

        # set audit log url
        vAuditLogUrl = (
            f"admin/activityevents?"
            f"startDateTime='{vCurrentStartIso8601}'&"
            f"endDateTime='{vCurrentEndIso8601}'"
        )

        # define the final url
        vUrl = vBaseUrl + vAuditLogUrl

        try:
            # start timer 
            start = timer()

            # run the extraction log function
            extract_audit_logs(vUrl, vHeader, df_audit_log, audit_date)  

            # logging
            end = timer()
            vElapsedTime = timedelta(seconds=end-start)
            vMessage = f"extraction of <{audit_date}> succeeded"
            dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'extraction of audit logs', datetime.now(), vElapsedTime, vMessage, ''] 

        except Exception as e:
            # logging
            end = timer()
            vElapsedTime = timedelta(seconds=end-start)
            vMessage = f"extraction of <{audit_date}> failed"
            dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'extraction of audit logs', datetime.now(), vElapsedTime, vMessage, str(e) ] 
            if pDebugMode == "yes":
                print(str(e))


else:

    # use the provided time frame 
    vHours = int(pAuditLogTimeframeInMinutes) // 60
    vHours = 0 if vHours == 1 else vHours
    vMinutes = int(pAuditLogTimeframeInMinutes) % 60
    vMinutes = 59 if vMinutes < 59 else vMinutes 
    vSseconds = 59



    # prepare the start and end date for the extraction
    vCurrentStart = datetime.strptime(pDateAndTime,vDateFormat)
    vCurrentEnd = vCurrentStart + timedelta(hours = vHours, minutes= vMinutes, seconds=vSseconds) 
    vCurrentStartIso8601 = vCurrentStart.strftime('%Y-%m-%dT%H:%M:%S.')+'000Z'
    vCurrentEndIso8601 = vCurrentEnd.strftime('%Y-%m-%dT%H:%M:%S.')+'000Z'

    # set audit log url
    vAuditLogUrl = (
        f"admin/activityevents?"
        f"startDateTime='{vCurrentStartIso8601}'&"
        f"endDateTime='{vCurrentEndIso8601}'"
    )

    # define the final url
    vUrl = vBaseUrl + vAuditLogUrl    

    try:
        # start timer 
        start = timer()

        # run the extraction log function
        extract_audit_logs(vUrl, vHeader, df_audit_log, pDateAndTime)  

        # logging
        end = timer()
        vElapsedTime = timedelta(seconds=end-start)
        vMessage = f"extraction of <{pDateAndTime}> succeeded"
        dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'extraction of audit logs', datetime.now(), vElapsedTime, vMessage, ''] 

    except Exception as e:
        # logging
        end = timer()
        vElapsedTime = timedelta(seconds=end-start)
        vMessage = f"extraction of <{pDateAndTime}> failed"
        dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'extraction of audit logs', datetime.now(), vElapsedTime, vMessage, str(e) ] 
        if pDebugMode == "yes":
            print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Format column names**

# CELL ********************

try:

    # get the column names
    columns = df_audit_log.columns.values.tolist()

    # convert to string
    df_audit_log[columns] = df_audit_log[columns].astype(str)

    # iterate over the column name and rename after capitalizing the first letter
    for columnName in columns:

        # split the column name, take the last item and upper case the first letter
        processed_column = process_column_name(columnName, '.')

        df_audit_log.rename(columns={columnName: processed_column}, inplace=True)


    # logging
    vMessage = "formating column names succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'formating column names', datetime.now(), None, vMessage, ''] 

except Exception as e:
    vMessage = f"formating column names failed."
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'formating column names', datetime.now(), None, vMessage, str(e) ] 
    if pDebugMode == "yes":
        print(str(e))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Saving to the lakehouse**

# CELL ********************

# if df_audit_log exists proceed further with formating
if not df_audit_log.empty:

    # start timer 
    start = timer()
    
    try:


        # create the ExtractionTime by rounding to the lowest hour
        df_audit_log['ExtractionTime'] = df_audit_log['CreationTime'].astype('datetime64[ns]').dt.floor('H')

        # convert from pandas to spark 
        spark_df_audit_log = spark.createDataFrame(df_audit_log) 

        # save to target
        spark_df_audit_log.write.mode("overwrite").format("delta").partitionBy("ExtractionTime").option("mergeSchema", "true").saveAsTable("staging.audit_log")


        # logging
        end = timer()
        vElapsedTime = timedelta(seconds=end-start)
        vMessage = "saving audit logs to lakehouse succeeded"
        dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'saving audit logs to lakehouse', datetime.now(), vElapsedTime, vMessage, ''] 

    except Exception as e:
        # logging
        end = timer()
        vElapsedTime = timedelta(seconds=end-start)
        vMessage = f"saving audit logs to lakehouse failed."
        dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'saving audit logs to lakehouse', datetime.now(), vElapsedTime, vMessage, str(e) ] 
        if pDebugMode == "yes":
            print(str(e))
else:
    vMessage = "df_audit_log dataframe does not exist"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'Activity Log processing', datetime.now(), '', vMessage, ''] 
    if pDebugMode == "yes":
        print(vMessage)

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

# **Return the audit log extraction datetime**

# CELL ********************

if pInitialization == "yes":
    # get the last date in the list of dates and add 1 day to it
    vLastDate = df_audit_dates.iloc[-1, -1] 
    vAuditLogExtractDateTime = datetime.strptime(vLastDate,vDateFormat)

else:

    # add 1 second to the time
    vAuditLogExtractDateTime = vCurrentEnd + timedelta(hours = 0, minutes= 0, seconds=1) 

# set the result
vResult = {
    "AuditLogExtractDateTime": vAuditLogExtractDateTime.strftime("%Y-%m-%d %H:%M:%S")
}

# exit
notebookutils.notebook.exit(str(vResult))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
