# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# **This notebook will do the following:**
# - Bind the notebooks to the monitoring lakehouse
# - Update connections in the data factory pipelines

# MARKDOWN ********************

# **Variables**

# CELL ********************

vLakehouseName = "MonitoringLake"
vWarehouseName = "LoadFramework"
vDebugMode = "yes"

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

# **Resolve current workspace name and id**

# CELL ********************

vWorkspaceName, vWorkspaceId = fabric.resolve_workspace_name_and_id()
print(vWorkspaceName, vWorkspaceId)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Define the default lakehouse in notebooks:**
# - nb_monitoring_staging_audit_log
# - nb_monitoring_staging_execution
# - nb_monitoring_staging_gateway_clusters
# - nb_monitoring_staging_tenant_metadata
# - nb_monitoring_staging_workspace_inventory

# CELL ********************

vLakehouseId = fabric.resolve_item_id(vLakehouseName)

# get the list of data pipelines in the target workspace
vNotebookList = [
    'nb_monitoring_staging_audit_log',
    'nb_monitoring_staging_execution',
    'nb_monitoring_staging_gateway_clusters',
    'nb_monitoring_staging_tenant_metadata',
    'nb_monitoring_staging_workspace_inventory'
]

df_notebooks = notebookutils.notebook.list(workspaceId=vWorkspaceId)
for notebook in df_notebooks:
    
    # get the notebook id and display name
    vNotebookId = notebook.id
    vNotebookName = notebook.displayName

    if vNotebookName in vNotebookList:

        # get the current notebook definition
        vNotebookDefinition = notebookutils.notebook.getDefinition(name=vNotebookName, workspaceId=vWorkspaceId) 
        vNotebookJson = json.loads(vNotebookDefinition)

        # update lakehouse dependencies
        try:

            # check and remove any attached lakehouses
            if 'dependencies' in vNotebookJson['metadata'] \
                and 'lakehouse' in vNotebookJson['metadata']['dependencies'] \
                and vNotebookJson['metadata']["dependencies"]["lakehouse"] is not None:

                vCurrentLakehouse = vNotebookJson['metadata']['dependencies']['lakehouse']
                # print(vCurrentLakehouse)

                if 'default_lakehouse_name' in vCurrentLakehouse:

                    vNotebookJson['metadata']['dependencies']['lakehouse'] = {}
                    print(f"attempting to update notebook <{vNotebookName}> with new default lakehouse: {vCurrentLakehouse['default_lakehouse_name']} in workspace <{vWorkspaceName}>.")

                    # update new notebook definition after removing existing lakehouses and with new default lakehouseId
                    notebookutils.notebook.updateDefinition(
                        name = vNotebookName,
                        content  = json.dumps(vNotebookJson),  
                        defaultLakehouse = vLakehouseName, #vCurrentLakehouse['default_lakehouse_name'],
                        defaultLakehouseWorkspace = vWorkspaceId,
                        workspaceId = vWorkspaceId
                    )

                    print(f"{icons.green_dot} updated notebook <{vNotebookName}> in workspace <{vWorkspaceName}>.")

                else:
                    print(f'no default lakehouse set for notebook <{vNotebookName}>, ignoring.')

            vMessage = f"succeeded"
        except Exception as e:
            vMessage = f"failed"
            if vDebugMode == "yes":
                print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Update connections in data factory pipelines**

# CELL ********************

# function to parse the json of the pipeline and update the Warehouse and Lakehouse linked services
def update_linked_services(obj):

    if isinstance(obj, dict):  # If the object is a dictionary

        if "linkedService" in obj and isinstance(obj["linkedService"], dict):
            properties = obj["linkedService"].get("properties", {})
            
            if properties.get("type") == "DataWarehouse":
                
                type_properties = properties.get("typeProperties", {})


                # get the target values 
                # source_artifact_name = fabric.resolve_item_name(item_id=vWarehouseName, workspace=vWorkspaceId)
                target_artifact_id = fabric.resolve_item_id(item_name=vWarehouseName, type='Warehouse', workspace=vWorkspaceId)
                artifact_url  = f"v1/workspaces/{vWorkspaceId}/warehouses/{target_artifact_id}"
                response = client.get(artifact_url)
                target_endpoint = response.json()['properties']['connectionString']
                target_values = {
                    "endpoint": f"{target_endpoint}",
                    "artifactId": f"{target_artifact_id}",
                    "workspaceId": f"{vWorkspaceId}"
                }

                # update the properties using the target values
                type_properties["endpoint"] = target_values["endpoint"]
                type_properties["artifactId"] = target_values["artifactId"]
                type_properties["workspaceId"] = target_values["workspaceId"]

            if properties.get("type") == "Lakehouse":
                
                type_properties = properties.get("typeProperties", {})


                # get the target values 
                # source_artifact_name = fabric.resolve_item_name(item_id = vLakehouseName, workspace=vWorkspaceId)
                target_artifact_id = fabric.resolve_item_id(item_name = vLakehouseName, type='Lakehouse', workspace=vWorkspaceId)
                target_values = {
                    "artifactId": f"{target_artifact_id}",
                    "workspaceId": f"{vWorkspaceId}"
                }

                # update the properties using the target values
                type_properties["artifactId"] = target_values["artifactId"]
                type_properties["workspaceId"] = target_values["workspaceId"]
        
        # Recursively search all keys in the dictionary
        for key in obj:
            update_linked_services(obj[key])
    
    elif isinstance(obj, list):  # If the object is a list, iterate over elements
        for item in obj:
            update_linked_services(item)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# function to update the data pipeline definition
def update_data_pipeline_definition(
    name: str, pipelineContent: dict, workspace: Optional[str] = None
):

    # resolve the workspace name and id
    (vWorkspace, vWorkspaceId) = resolve_workspace_name_and_id(workspace)

    # get the pipeline payload
    vPipelinePayload = base64.b64encode(json.dumps(pipelineContent).encode('utf-8')).decode('utf-8')
    
    # resolve the pipeline id
    vPipelineId = fabric.resolve_item_id(item_name=name, type="DataPipeline", workspace=vWorkspace)

    # prepare the request body
    vRequestBody = {
        "definition": {
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": vPipelinePayload,
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }

    # response
    vResponse = client.post(
        f"v1/workspaces/{vWorkspaceId}/items/{vPipelineId}/updateDefinition",
        json=vRequestBody,
    )

    lro(client, vResponse, return_status_code=True)

    print(f"{icons.green_dot} the '{name}' pipeline was updated within the '{vWorkspace}' workspace.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# get the list of data pipelines in the target workspace
df_pipeline = labs.list_data_pipelines(vWorkspaceName)

# iterate over the data pipelines
for index, row in df_pipeline.iterrows():

    vPipelineName = row['Data Pipeline Name']

    # retrieve the pipeline json definition
    vPipelineJson = json.loads(labs.get_data_pipeline_definition(vPipelineName, vWorkspaceName))
    # print(json.dumps(vPipelineJson, indent=4))


    # update linked services
    try:
        update_linked_services(vPipelineJson.get("properties", {}).get("activities", []))
    except Exception as e:
        vMessage = f"failed"
        if vDebugMode == "yes":
            print(str(e))
            

    # update pipeline definition
    try:
        update_data_pipeline_definition(name=vPipelineName,pipelineContent=vPipelineJson, workspace=vWorkspaceName)
    except Exception as e:
        vMessage = f"failed"
        if vDebugMode == "yes":
            print(str(e))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
