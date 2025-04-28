# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f2edcb85-68a4-4422-a767-8ed894ec5945",
# META       "default_lakehouse_name": "MonitorLake",
# META       "default_lakehouse_workspace_id": "ff293ed8-1d93-4b88-8007-fb51437c4979",
# META       "known_lakehouses": [
# META         {
# META           "id": "f2edcb85-68a4-4422-a767-8ed894ec5945"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Install semantic link labs**

# CELL ********************

!pip install semantic-link-labs --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Libraries**

# CELL ********************

import base64
import datetime as dt
import json
import os
import re
import struct
import time
import sys
import math
from datetime import datetime, timedelta, time
from string import Template
from timeit import default_timer as timer
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import pyodbc
import requests
import sqlalchemy
from IPython.display import Markdown, display
from notebookutils import mssparkutils
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

import sempy.fabric as fabric
import sempy_labs as labs
import sempy_labs._icons as icons
from sempy import fabric
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException
from sempy_labs._helper_functions import _decode_b64, lro, resolve_workspace_name_and_id

from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# import sempy.fabric as fabric
# import sempy_labs as labs
# from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException
# import json
# import requests
# import pandas as pd
# import os
# import datetime as dt
# import time
# from timeit import default_timer as timer
# from datetime import datetime, timedelta
# from string import Template
# import base64
# import re
# import struct
# import sqlalchemy
# import pyodbc
# from notebookutils import mssparkutils
# import numpy as np
# import sempy_labs as labs
# from sempy import fabric
# import com.microsoft.spark.fabric
# from com.microsoft.spark.fabric.Constants import Constants
# from IPython.display import display, Markdown
# from pyspark.sql import DataFrame
# from pyspark.sql.functions import col,current_timestamp,lit
# from typing import Optional, Tuple, List
# from sempy_labs._helper_functions import (resolve_workspace_name_and_id, lro, _decode_b64)
# import sempy_labs._icons as icons

# MARKDOWN ********************

# **Fabric client**

# CELL ********************

client = fabric.FabricRestClient()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function to flatten a nested json**
# - Takes a pandas dataframe
# - Search for columns of type list
# - flatten the list

# CELL ********************

def flatten_nested_json_df(df):

    df = df.reset_index()

    # search for columns to explode/flatten
    s = (df.applymap(type) == list).all()
    list_columns = s[s].index.tolist()

    s = (df.applymap(type) == dict).all()
    dict_columns = s[s].index.tolist()

    while len(list_columns) > 0 or len(dict_columns) > 0:
        new_columns = []

        for col in dict_columns:

            horiz_exploded = pd.json_normalize(df[col]).add_prefix(f'{col}.')
            horiz_exploded.index = df.index
            df = pd.concat([df, horiz_exploded], axis=1).drop(columns=[col])
            new_columns.extend(horiz_exploded.columns) # inplace

        for col in list_columns:

            # explode lists vertically, adding new columns
            df = df.drop(columns=[col]).join(df[col].explode().to_frame())
            new_columns.append(col)

        # check if there are still dict o list fields to flatten
        s = (df[new_columns].applymap(type) == list).all()
        list_columns = s[s].index.tolist()

        s = (df[new_columns].applymap(type) == dict).all()
        dict_columns = s[s].index.tolist()

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function to upper case the first letter of a string**

# CELL ********************

def convert_into_uppercase(string_val):
    return string_val.group(1) + string_val.group(2).upper()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function to split a string, take the last 2 items and capitalize their first character**
# 
# Example: 
# input   -> 'datamarts.users.datamartUserAccessRight'
# output  -> 'UsersDatamartUserAccessRight'
# 
# or 
# input   -> 'datamarts.users'
# output  -> 'DatamartsUsers'

# CELL ********************

def process_column_name(column_name, separator):
    list_values = column_name.split(separator)

    len_list = len(list_values)

    # iterate over the list
    for i in range(len(list_values)):

        # current value 
        current_value = list_values[i]

        # upper case the first letter 
        upper_case_value = re.sub("(^|\s)(\S)", convert_into_uppercase, current_value) 

        # replace the column name in the dataframe
        list_values[i] = upper_case_value

    list_values_joined = ''.join(list_values)
    return list_values_joined

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Time rounder**

# CELL ********************

# function to round to the nearest 15min
def fnRoundMinDatetime(dt, delta):
    return datetime.min + round((dt - datetime.min) / delta) * delta

def fnRoundHourDatetime(dt):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (dt.replace(second=0, microsecond=0, minute=0, hour=dt.hour)
               +timedelta(hours=dt.minute//30))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function to add a user to a fabric workspace**

# CELL ********************

def add_user_to_fabric_workspace(baseUrl, workspaceId, userUPN, accessToken, waitTime):

    vWorkspaceId = workspaceId
    vBaseUrl = baseUrl
    vUserUPN = userUPN
    vAccessToken = accessToken
    vWaitTime = waitTime

    # log activity
    vMessage = f"adding user <{vUserUPN}> as admin to workspace <{vWorkspaceId}>"
    print(vMessage)

    # inputs for post request
    vHeader = {'Content-Type':'application/json','Authorization': f'Bearer {vAccessToken}'} 
    vJsonBody = {
        "groupUserAccessRight": "Admin",
        "emailAddress": vUserUPN
    }
    vAssignUrl = "admin/groups/" + vWorkspaceId + "/users"


    try:
        # post the assignment
        assignment_response = requests.post(vBaseUrl + vAssignUrl, headers=vHeader, json=vJsonBody)

        # raise an error for bad status codes
        assignment_response.raise_for_status()  

        # get the status code and reason
        status_code = assignment_response.status_code
        status = assignment_response.reason

        # check status
        if status_code == 200: 

            vMessage = f"assigning user <{vUserUPN}> to workspace <{vWorkspaceId}> succeeded."
            print(f"{vMessage}")
            status = "succeeded"
            print(f"sleeping {vWaitTime} seconds")
            time.sleep(vWaitTime) # to avoid hitting the limit of the api

    except requests.exceptions.HTTPError as errh: 
        error_message = errh.args[0]
        vMessage = f"assigning user <{vUserUPN}> to workspace <{vWorkspaceId}> failed. HTTP Error; error: <{error_message}>"
        print(f"{vMessage}")
        status = "failed"

    except requests.exceptions.ReadTimeout as errrt: 
        vMessage = f"assigning user <{vUserUPN}> to workspace <{vWorkspaceId}> failed. Time out; error: <{errrt}>"
        print(f"{vMessage}")
        status = "failed"

    except requests.exceptions.ConnectionError as conerr: 
        vMessage = f"assigning user <{vUserUPN}> to workspace <{vWorkspaceId}> failed. Connection error; error: <{conerr}>"
        print(f"{vMessage}")
        status = "failed"

    except requests.exceptions.RequestException as errex: 
        vMessage = f"assigning user <{vUserUPN}> to workspace <{vWorkspaceId}> failed. Exception request; error: <{errex}>"
        print(f"{vMessage}")
        status = "failed"


    # return the status
    return status




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function to remove a user from a fabric workspace**

# CELL ********************

def remove_user_from_fabric_workspace(baseUrl, workspaceId, userUPN, accessToken, waitTime):

    vWorkspaceId = workspaceId
    vBaseUrl = baseUrl
    vUserUPN = userUPN
    vAccessToken = accessToken
    vWaitTime = waitTime

    # log activity
    vMessage = f"deleting user <{vUserUPN}> from workspace <{vWorkspaceId}>"
    print(vMessage)


    # inputs for post request
    vHeader = {'Content-Type':'application/json','Authorization': f'Bearer {vAccessToken}'} 
    vDeleteUrl = "admin/groups/" + vWorkspaceId + "/users/" + vUserUPN

    try:
        # post the assignment
        assignment_response = requests.delete(vBaseUrl + vDeleteUrl, headers=vHeader)

        # raise an error for bad status codes
        assignment_response.raise_for_status()  

        # get the status code and reason
        status_code = assignment_response.status_code
        status = assignment_response.reason

        # check status
        if status_code == 200: 

            vMessage = f"deleting user <{vUserUPN}> from workspace <{vWorkspaceId}> succeeded."
            print(f"{vMessage}")
            status = "succeeded"
            print(f"sleeping {vWaitTime} seconds")
            time.sleep(vWaitTime) # to avoid hitting the limit of the api


    except requests.exceptions.HTTPError as errh: 
        error_message = errh.args[0]
        vMessage = f"deleting user <{vUserUPN}> from workspace <{vWorkspaceId}> failed. HTTP Error; error: <{error_message}>"
        print(f"{vMessage}")
        status = "failed"

    except requests.exceptions.ReadTimeout as errrt: 
        vMessage = f"deleting user <{vUserUPN}> from workspace <{vWorkspaceId}> failed. Time out; error: <{errrt}>"
        print(f"{vMessage}")
        status = "failed"

    except requests.exceptions.ConnectionError as conerr: 
        vMessage = f"deleting user <{vUserUPN}> from workspace <{vWorkspaceId}> failed. Connection error; error: <{conerr}>"
        print(f"{vMessage}")
        status = "failed"

    except requests.exceptions.RequestException as errex: 
        vMessage = f"deleting user <{vUserUPN}> from workspace <{vWorkspaceId}> failed. Exception request; error: <{errex}>"
        print(f"{vMessage}")
        status = "failed"


    # return the status
    return status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function to call a fabric api and return the correspondant dataframe**

# CELL ********************

def perform_get_request(url, headers, debug_mode, extraction_type):

    # the fabric client fails when making api calls to the onelake storage
    if extraction_type=="file_system":
        try:
            response = requests.get(url, headers = headers)
            return response
        except Exception as e:
            print("failed to call the api. exception:", str(e))
            return None
    else:
        try:
            response = client.get(url, headers)

            if response.status_code != 200:
                raise FabricHTTPException(response)
            else:
                return response

        except FabricHTTPException as e:
            if debug_mode == "yes":
                print("failed to call the fabric api. exception:", str(e))
            return None


def handle_response(response, debug_mode):
    if response is None:
        if debug_mode == "yes":
            print("response is None")
        return None
    elif not response.text.strip():
        if debug_mode == "yes":
            print("response is empty")
        return None
    else:
        try:
            # convert response to JSON
            response_data = response.json()
            response_content = json.loads(response.content)
            continuation_token = "" #response_data.get('continuationToken', None)
            continuation_uri = "" #response_data.get('continuationUri', None)
            return response_content, continuation_token, continuation_uri
        except ValueError:
            if debug_mode == "yes":
                print("failed to parse response as json")
            return None


def json_to_dataframe(response_content, debug_mode, extraction_type):
    try:
        match extraction_type:           
            case "audit_logs":
                result_dataframe = pd.concat([pd.json_normalize(x) for x in response_content['activityEventEntities']])
                return result_dataframe
            case "domains":
                result_dataframe = pd.concat([pd.json_normalize(x) for x in response_content['domains']])
                return result_dataframe
            case "external_data_shares":
                result_dataframe = pd.concat([pd.json_normalize(x) for x in response_content['value']])
                return result_dataframe
            case "tenant_settings":
                result_dataframe = pd.concat([pd.json_normalize(x) for x in response_content['tenantSettings']])
                return result_dataframe
            case "capacities":
                result_dataframe = pd.concat([pd.json_normalize(x) for x in response_content['value']])
                return result_dataframe
            case "connections":
                result_dataframe = pd.concat([pd.json_normalize(x) for x in response_content['value']])
                return result_dataframe
            case "deployment_pipelines":
                result_dataframe = pd.concat([pd.json_normalize(x) for x in response_content['value']])
                return result_dataframe
            case "gateways":
                result_dataframe = pd.concat([pd.json_normalize(x) for x in response_content['value']])
                return result_dataframe
            case "shortcuts":
                result_dataframe = pd.concat([pd.json_normalize(x) for x in response_content['value']])
                return result_dataframe
            case "file_system":
                result_dataframe = pd.concat([pd.json_normalize(x) for x in response_content['paths']])
                return result_dataframe
            case "onelake_access":
                result_dataframe = pd.concat([pd.json_normalize(x) for x in response_content['value']])
                return result_dataframe
            # case "relations":
            #     result_dataframe = pd.concat([pd.json_normalize(x) for x in response_content['relations']])
            #     return result_dataframe   

    except Exception as e:
        if debug_mode == "yes":
            print(f"failed to generate the required dataframe. exception: {str(e)}")      
              

def append_to_global_df(result_dataframe):
    global api_call_global_dataframe
    if not result_dataframe.empty:
        api_call_global_dataframe = pd.concat([api_call_global_dataframe, result_dataframe], ignore_index=True)

def api_call_main(url, headers, debug_mode, extraction_type):

    # set boolean vaule to continue to the next interval (in case response has a paging url)
    continue_to_next_interval = True

    # while loop the boolean is true
    while continue_to_next_interval:

        # perform the GET request
        response = perform_get_request(url, headers, debug_mode, extraction_type)
        # print(json.loads(response.text))

        # # handle the response
        response_content, continuation_token, continuation_uri = handle_response(response, debug_mode)
        # print(response_content, continuation_token, continuation_uri)

        # convert to a dataframe
        result_dataframe = json_to_dataframe(response_content, debug_mode, extraction_type)

        # append to the global dataframe
        append_to_global_df(result_dataframe)

        # while there is a continuation token, request the next continuation url
        # continuation_count = 0
        while continuation_token:
            # continuation_count +=1
            # print(f"continuation {continuation_count}")
            response = perform_get_request(continuation_uri, headers, debug_mode, extraction_type) 
            response_content, continuation_token, continuation_uri = handle_response(response, debug_mode)
            result_dataframe = json_to_dataframe(response_content, debug_mode, extraction_type)
            append_to_global_df(result_dataframe)

        # if no error exit the while loop
        continue_to_next_interval = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function to create a fabric item**

# CELL ********************

def create_or_update_fabric_item(url, headers, body, call_type, operation, workspace_id, item_name, item_type, sleep_in_seconds, debug_mode):

    vMessage = f"{operation} {item_type} <{item_name}> in workspace <{workspace_id}>"
    print(vMessage)
    
    if call_type == "post":

        # # json body
        # vJsonBody = {
        #     "displayName": f"{item_name}",
        #     "type": f"{item_type}",
        #     "description": f"{item_type} {item_name} created by fabric notebook"
        # }

        try:
            # post the assignment
            if body is None:
                response = client.post(url, headers=headers)
            else:
                response = client.post(url, headers=headers, json=body)

            if response.status_code not in (200, 201, 202):
                raise FabricHTTPException(response)
            else:

                # check status
                if response.status_code == 201: # if status is 201 then the create item succeeded

                    vMessage = f"{operation} {item_type} <{item_name}> in workspace <{workspace_id}> succeeded"
                    print(f"{vMessage}")

                elif response.status_code == 202: # if status is 202 then the create item is in progress
                    
                    vMessage = f"{operation} {item_type} <{item_name}> in workspace <{workspace_id}> is in progress"
                    print(vMessage)

                    # get the operation url from the header location
                    # doc https://learn.microsoft.com/en-us/rest/api/fabric/articles/long-running-operation
                    operation_url = response.headers.get("Location")

                    # vMessage = f"operation url: <{operation_url}>"

                    # monitor the operation
                    while True:

                        # sleep the specified time --> this wait time might need adjustment
                        time.sleep(sleep_in_seconds)  

                        # check the operation
                        operation_response = client.get(operation_url, headers=headers) 

                        if operation_response.status_code == 200:

                            vMessage = f"{operation} {item_type} <{item_name}> in workspace <{workspace_id}> succeeded"
                            print(f"{vMessage}")
                            break

                        else:
                            vMessage = f"{operation} {item_type} <{item_name}> in workspace <{workspace_id}> failed"
                            print(f"{vMessage}")
                            break

                else: # any other status is a failure
                    vMessage = f"{operation} {item_type} <{item_name}> in workspace <{workspace_id}> failed"
                    print(f"{vMessage}")

                    # retry: 
                    vMessage = f"second attempt - {operation} {item_type} <{item_name}> in workspace <{workspace_id}>"
                    print(f"{vMessage}")
                    create_item(url, headers, body, operation, workspace_id, item_name, item_type, sleep_in_seconds, debug_mode)

        except FabricHTTPException as e:
            print("failed to call the fabric api. exception:", str(e))
            return None
    else:
        try:
            response = requests.put(url, headers=headers, json=body)
            print(response.text)
        except Exception as e:
            print("failed to call the fabric api. exception:", str(e))
            return None



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function to recursively replace placeholders in a json object**

# CELL ********************

def replace_placeholders_in_json(obj, inputs_for_json):
    if isinstance(obj, dict):
        return {k: replace_placeholders_in_json(v, inputs_for_json) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_placeholders_in_json(item, inputs_for_json) for item in obj]
    elif isinstance(obj, str):
        for key, value in inputs_for_json.items():
            obj = obj.replace(f"{{{key}}}", str(value))
        return obj
    else:
        return obj

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
