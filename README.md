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
