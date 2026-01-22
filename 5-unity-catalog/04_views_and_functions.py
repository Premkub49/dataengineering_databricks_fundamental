# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4: Views and Functions
# MAGIC
# MAGIC **Objective:** Learn to encapsulate logic and simplify data access.
# MAGIC *   Identify all Databricks table types: Managed, External, Views, Materialized Views, and Streaming Tables.
# MAGIC *   Create **Standard Views** to simplify complex queries.
# MAGIC *   Create and use **SQL User Defined Functions (UDFs)** for reusable logic.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC Before we dive into Views, let's review the different types of "tables" you will encounter in Databricks and Unity Catalog.
# MAGIC
# MAGIC ### 1. Managed Tables
# MAGIC *   **Definition**: Databricks manages both the metadata and the underlying data files.
# MAGIC *   **Storage**: Data is stored in the root storage location of the Metastore or Catalog.
# MAGIC *   **Behavior**: If you `DROP TABLE`, the data is deleted from disk.
# MAGIC
# MAGIC ### 2. External Tables
# MAGIC *   **Definition**: Databricks manages the metadata, but the data files live in your own cloud storage (S3/ADLS/GCS).
# MAGIC *   **Storage**: You provide a `LOCATION` when creating the table.
# MAGIC *   **Behavior**: If you `DROP TABLE`, only the metadata is removed; the data files remain.
# MAGIC
# MAGIC ### 3. Standard Views
# MAGIC *   **Definition**: A virtual table defined by a SQL query. It does **not** store data on disk.
# MAGIC *   **Behavior**: The query is executed every time you select from the view. Great for security and simplifying complex joins.
# MAGIC
# MAGIC ### 4. Materialized Views (MV)
# MAGIC *   **Definition**: A view that computes and stores its results on disk (like a table).
# MAGIC *   **Behavior**: It can be refreshed on a schedule. It provides the performance of a table with the logic of a view. (Common in Databricks SQL and Delta Live Tables).
# MAGIC
# MAGIC ### 5. Streaming Tables
# MAGIC *   **Definition**: Tables designed for incremental data processing.
# MAGIC *   **Behavior**: They automatically track which data has been processed and only handle new data. (Primarily used in Delta Live Tables).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC In this module, we will focus on **Standard Views** and **User Defined Functions (UDFs)**.
# MAGIC
# MAGIC
# MAGIC **note: always change the Serverless Environment version to version 4**

# COMMAND ----------

# Setup variables
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username_suffix = current_user.split("@")[0].replace(".", "_")
catalog_name = f"training_catalog_{username_suffix}"
dbutils.widgets.text("catalog_name", catalog_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG IDENTIFIER(:catalog_name);
# MAGIC USE SCHEMA learning_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create a Standard View
# MAGIC
# MAGIC Let's create a view that hides the email address from our `managed_users` table.
# MAGIC **(Table from workshop 03)**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW public_user_info AS
# MAGIC SELECT id, name FROM managed_users;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the view
# MAGIC SELECT * FROM public_user_info;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED public_user_info;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: User Defined Functions (UDFs)
# MAGIC
# MAGIC You can create SQL functions that are stored in Unity Catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a function to mask email
# MAGIC CREATE OR REPLACE FUNCTION mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC   WHEN email IS NULL THEN NULL
# MAGIC   ELSE CONCAT(LEFT(email, 1), '****', RIGHT(email, 4))
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Use the UDF
# MAGIC
# MAGIC Now we can use this function in any query, just like a built-in function.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   name, 
# MAGIC   email, 
# MAGIC   mask_email(email) as masked_email 
# MAGIC FROM managed_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC 1. Create a view named `masked_users_view` that uses the `mask_email` function to show the ID, Name, and Masked Email.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Your code here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC We have data and logic. How do we control who sees it? Let's look at Governance.
