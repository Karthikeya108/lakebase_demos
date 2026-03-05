*** Overall Project Instruction ***
The goal is to create an app that will show the performance difference between Databricks DBSQL and Databricks Lakebase Autoscaling for a small transactional use case. The use case is to read, write or update data throuhg Databricks App application. The project should have mutliple tables related to insurance domain. 10 tables with approxiately 100,000 records per table. 
- Reseach the web for as suitable data model from the insurance domain, preferably from the re-insurance domain.
- Create tables in Unity Catalog catalog "tko_2026" and schema "lakebase_demo" with generated data, approx. 10 tables with and average of 100,000 records per tables for most tables and some small tables. Overall the dataset should make sense and the tables should related to each other with primary and foreign keys.
- Once the dataset is ready with proper data model create a suitable SQL warehouse with photon acceleration enabled.
- Create a Lakebase Autoscaling project with decent minimum and maximum CUs. Make sure you create the Autoscaling version of the Lakebase and not the provisioned version. For details refer to the documentation: https://learn.microsoft.com/en-us/azure/databricks/oltp/projects/
- Sync the tables that you created in Unity Catalog to the lakebase project. Create a new database tko_2026_demo
- Create a simple Databricks App using the apx project as a template: https://github.com/databricks-solutions/apx
- Update the app to show the options which are: Lakehouse and Lakebase
- The Lakehouse option should connect to the DBSQL and use the delta tables in Unity Catalog
- The Lakebase option shouold connect to the Lakebase instance and use the tables in Lakebase
- Update the app that you created to display the tables, select the table, view the contents with 10 records per page and use pagination. Allow updating a field, which should be written back to the table. 