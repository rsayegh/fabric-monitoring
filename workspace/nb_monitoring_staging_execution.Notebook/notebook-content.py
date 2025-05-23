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
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# **Libraries**

# CELL ********************

import pandas as pd
from datetime import datetime, timedelta
from timeit import default_timer as timer
import sempy.fabric as fabric
import json
import ast

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

# **Parameters**

# PARAMETERS CELL ********************

pLoadId = "1"
pConfiguration = '{"pConfigId":1,"pConfigName":"Initialization","pAuditLogTimeframeInMinutes":"60","pAllActivities":"yes","pInitialization_Audit":"yes","pAuditLog":"yes","pLastProcessedDateAndTime_Audit":"2025-04-27 00:00:00","pKeyVaultName":"rs-vault-dev","pTenantId":"AzureTenantId","pDomainName":"DomainName","pFabricSpnClientId":"FabricSpnId","pFabricSpnSecret":"FabricSpnSecret","pFabricSpnAdminConsentClientId":"FabricSpnAdminConsentId","pFabricSpnAdminConsentSecret":"FabricSpnAdminConsentSecret","pFabricSecurityGroupId":"FabricSecurityGroupId","pReloadDates":"yes","pStartDate":"2025-01-01","pEndDate":"2025-12-31","pInitialization_Inventory":"yes","pThrottleScanApi":"yes","pLastProcessedDateAndTime_Inventory":"2025-04-27 00:00:00","pTenantMetadata":"yes","pGatewayClusters":"yes","pInventory":"yes","pDatasetRefreshHistory":"yes","pTopNRefreshHistory":"0"}'
pDebugMode = "no"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Extract the parameters from the configuration**

# CELL ********************

vConfiguration = json.loads(pConfiguration)

# Dynamically create variables from JSON keys
for key, value in vConfiguration.items():
    globals()[key] = value
    print(f"{key}: {value}")

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
    vSqlAccessToken = vAccessToken
else:
    # when the code is run from the pipelines, to token is generated in a previous step and passed as a parameter to the notebook
    vAccessToken = notebookutils.notebook.run(
        "nb_authentication", 
        300,
        {
            "useRootDefaultLakehouse": True,
            "pKeyVaultName" : pKeyVaultName,
            "pTenantId": pTenantId,
            "pFabricSpnClientId" : pFabricSpnClientId,                    
            "pFabricSpnSecret" : pFabricSpnSecret                                                                              
        }
    )

    vAccessToken_v2_apis = notebookutils.notebook.run(
        "nb_authentication", 
        300,
        {
            "useRootDefaultLakehouse": True,
            "pKeyVaultName" : pKeyVaultName,
            "pTenantId": pTenantId,
            "pFabricSpnClientId" : pFabricSpnAdminConsentClientId,                    
            "pFabricSpnSecret" : pFabricSpnAdminConsentSecret                                                                              
        }
    )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Define the DAG**

# CELL ********************

dagList = []

# add to the DAG list nb_monitoring_staging_audit_log
if pAuditLog == "yes":
    dagList.append({
                "name": "nb_monitoring_staging_audit_log",
                "path": "nb_monitoring_staging_audit_log",
                "timeoutPerCellInSeconds": 900,
                "args": {
                    "useRootDefaultLakehouse": True,
                    "pLoadId" : pLoadId,
                    "pToken": vAccessToken,
                    "pDebugMode" : pDebugMode,                    
                    "pAuditLogTimeframeInMinutes" : pAuditLogTimeframeInMinutes,
                    "pAllActivities" : pAllActivities,
                    "pInitialization" : pInitialization_Audit,
                    "pDateAndTime" : pLastProcessedDateAndTime_Audit                                                                                
                    }
            })

# add to the DAG list nb_monitoring_staging_tenant_metadata
if pTenantMetadata == "yes":
    dagList.append({
                "name": "nb_monitoring_staging_tenant_metadata",
                "path": "nb_monitoring_staging_tenant_metadata",
                "timeoutPerCellInSeconds": 300,
                "args": {
                    "useRootDefaultLakehouse": True,
                    "pLoadId" : pLoadId,
                    "pToken": vAccessToken,
                    "pDebugMode" : pDebugMode
                    }
            })

# add to the DAG list nb_monitoring_staging_gateway_clusters
if pGatewayClusters == "yes":
    dagList.append({
                "name": "nb_monitoring_staging_gateway_clusters",
                "path": "nb_monitoring_staging_gateway_clusters",
                "timeoutPerCellInSeconds": 300,
                "args": {
                    "useRootDefaultLakehouse": True,
                    "pLoadId" : pLoadId,
                    "pToken" : vAccessToken_v2_apis,
                    "pDebugMode" : pDebugMode
                    }
            })



# add to the DAG list nb_monitoring_staging_workspace_inventory
if pInventory == "yes":
    dagList.append({
                "name": "nb_monitoring_staging_workspace_inventory",
                "path": "nb_monitoring_staging_workspace_inventory",
                "timeoutPerCellInSeconds": 300,
                "args": {
                    "useRootDefaultLakehouse": True,
                    "pLoadId" : pLoadId,
                    "pToken": vAccessToken,
                    "pDebugMode" : pDebugMode,                    
                    "pInitialize" : pInitialization_Inventory,
                    "pExportInventoryExpressions" : "yes",
                    "pThrottleScanApi" : pThrottleScanApi,
                    "pDateAndTime" : pLastProcessedDateAndTime_Inventory                                                                               
                    }
            })


DAG = { "activities": dagList,"concurrency": 1, "timeoutInSeconds": 900 }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.validateDAG(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Run multiple**

# CELL ********************

try:


    # start timer 
    start = timer()    

    results = notebookutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": True})

    # logging
    end = timer()
    vElapsedTime = timedelta(seconds=end-start)

    # logging
    vMessage = f"succeeded"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'running the DAG', datetime.now(), vElapsedTime, vMessage, ''] 

except Exception as e:
    vMessage = f"failed"
    dfLogging.loc[len(dfLogging.index)] = [pLoadId, vNotebookId, vLogNotebookName, vWorkspaceId, 'running the DAG', datetime.now(), None, vMessage, str(e)]
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
    sparkDF_Logging = spark.createDataFrame(dfLogging) 

    # save to the lakehouse
    sparkDF_Logging.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("staging.notebook_logging")

except Exception as e:
    vMessage = "saving logs to the lakehouse failed"
    if pDebugMode == "yes":
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Return the last extraction time**

# CELL ********************

vResult = {}

for notebook, result in results.items():
    exit_val_str = result.get('exitVal')
    if exit_val_str:
        try:
            # Try to parse the exitVal
            exit_val_dict = ast.literal_eval(exit_val_str)

            # Only update if it's actually a dictionary
            if isinstance(exit_val_dict, dict):
                vResult.update(exit_val_dict)

        except (ValueError, SyntaxError):
            # Skip if parsing fails
            print(f"Warning: Failed to parse exitVal for notebook '{notebook}'.")


notebookutils.notebook.exit(str(vResult))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
