# Databricks notebook source
# MAGIC %md
# MAGIC # Module 1: Introduction and Setup
# MAGIC
# MAGIC **Objective:** Understand the fundamental architecture of Unity Catalog.
# MAGIC *   Learn the **3-level namespace** (`catalog.schema.table`).
# MAGIC *   Explore the workspace to identify current catalogs and schemas.
# MAGIC *   Understand the difference between the `system` catalog and user-defined catalogs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC ## Welcome to the Unity Catalog Course!
# MAGIC
# MAGIC This course is designed to take you from a beginner to a confident user of Databricks Unity Catalog.
# MAGIC
# MAGIC ### What is Unity Catalog?
# MAGIC Unity Catalog is a unified governance solution for all data and AI assets including files, tables, machine learning models, and dashboards in your lakehouse on any cloud.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC 1.  **Unified Governance**: Manage permissions for data and AI models from a single place.
# MAGIC 2.  **3-Level Namespace**: `catalog.schema.table`
# MAGIC     *   **Catalog**: The top level container.
# MAGIC     *   **Schema (Database)**: Contains tables, views, etc.
# MAGIC     *   **Table/Asset**: The actual data or object.
# MAGIC 3.  **Data Lineage**: Automatically tracks how data flows between tables.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC *   A Databricks Workspace with Unity Catalog enabled.
# MAGIC *   Permission to create Catalogs (or a provided Catalog to work in).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Explore the Environment
# MAGIC
# MAGIC Let's see which catalog and schema we are currently using.
# MAGIC
# MAGIC **note: always change the Serverless Environment version to version 4**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current catalog
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current schema
# MAGIC SELECT current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: The 3-Level Namespace
# MAGIC
# MAGIC In Unity Catalog, you always reference data using the 3-level namespace:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM catalog_name.schema_name.table_name
# MAGIC ```
# MAGIC
# MAGIC If you don't specify the catalog or schema, Databricks uses the "current" ones selected above.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: List Available Catalogs
# MAGIC
# MAGIC Let's see what catalogs are available to you. You should see at least `main` or `hive_metastore` (legacy), and `system`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC 1. Run the cells above.
# MAGIC 2. Identify the `system` catalog in the output. This catalog contains audit logs and billing info.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC In the next notebook, we will create our own Catalog and Schema to start building our data structure.
