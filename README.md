# Overview

This wiki page provides a high-level overview of the current solution and how it leverages several new Microsoft Fabric capabilities.

## Key Components Used

- **Warehouse**
- **Lakehouse**
- **Data Factory**
- **Notebooks**

## How the Solution Works

A **Warehouse** item named `LoadFramework` is created to support the data loading process. It also provides a logging mechanism for the Data Factory pipelines.  
> **Note:** This warehouse is *not* used to store extracted data or serve as a traditional data warehouse or datamart. Work is in progress to replace it with a Fabric SQL Database.

The extracted data is stored in a **Lakehouse** item called `MonitoringLake`, which serves as the central data repository.

Data extractions are driven by **Data Factory** pipelines, starting with a pipeline named `pl_monitoring_master`. These pipelines rely on configurations stored in the `LoadFramework` database to determine:

- What data to extract
- How to run the extraction

Each configuration includes specific inputs needed for orchestration and notebook execution.  
For example, a configuration might be designed to extract only audit logs and would include only the relevant parameters for that task.

> See the corresponding wiki pages for more details on configuration.

The extraction logic is implemented in **Notebooks**, primarily written in Python. In some cases, they may also include **Spark SQL** queries to interact with data stored in the lakehouse.

# Architecture

<p align="center">
  <img src="resources/Architecture.png" alt="Architecture Diagram" width="600"/>
</p>
<p align="center"><em>Figure 1: High-level architecture overview</em></p>

# Load Framework & Azure Setup

## Azure Setup

This solution makes use of the following Azure components:

- **Service Principals**
- **Azure Key Vault**

### Service Principals

The solution uses two different **Service Principals**, each configured with specific API permissions.

Depending on the type of operation being performed in the notebooks, authentication with the fabric api may require one or both of these SPNs.

- **Fabric SPN**

  This is an SPN that does not require admin consent at the tenant level. It uses the following API permissions:

<p align="center">
  <img src="resources/Azure - Fabric SPN - Permissions.png" alt="Fabric SPN" width="600"/>
</p>
<p align="center"><em>Figure 2: Fabric SPN API permissions</em></p>


- **Fabric SPN Admin Consent**

  This is an SPN that requires admin consent at the tenant level. It uses the following API permissions:

<p align="center">
  <img src="resources/Azure - Fabric SPN Admin Consent - Permissions.png" alt="Fabric SPN" width="600"/>
</p>
<p align="center"><em>Figure 2: Fabric SPN Amin Consent - API permissions</em></p>

### Azure Key Vault

An **Azure Key Vault** is required for the solution. It will hold secrets for authenticating with the Fabric APIs.

In a key vault of your choice, create the following secrets:

<br />

  | Secret Name | Secret Value | Comment |
  | -------- | ------- | ------- |
  | AzureTenantId | <strong>your azure tenant id</strong> |  |
  | DomainName | <strong>your domain name</strong> |  |
  | FabricSpnId | <strong>the client id of the Fabric SPN app</strong> |  |
  | FabricSpnSecret | <strong>the secret of the Fabric SPN app</strong> |  |
  | FabricSpnAdminConsentId | <strong>the client id of the Fabric SPN Admin Consent app</strong> |  |
  | FabricSpnAdminConsentSecret | <strong>the secret of the Fabric SPN Admin Consent app</strong> |  |
  | FabricSecurityGroupId | <strong> security group id</strong> | a security group in which the 2 fabric SPNs are added |

<br />

