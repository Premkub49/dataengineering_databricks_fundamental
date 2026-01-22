# Databricks notebook source
# MAGIC %md
# MAGIC # Module 3: Managed vs. External Tables
# MAGIC
# MAGIC **Objective:** Understand data storage and lifecycle management.
# MAGIC *   Learn the difference between **Managed Tables** (UC manages files) and **External Tables** (User manages files).
# MAGIC *   Understand what happens to data when a table is dropped.
# MAGIC *   Learn how to create tables using SQL and CTAS (Create Table As Select).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC In Unity Catalog, there are two primary types of tables:
# MAGIC
# MAGIC 1.  **Managed Tables**:
# MAGIC     *   Databricks manages the underlying data files in cloud storage.
# MAGIC     *   When you `DROP TABLE`, the data files are deleted.
# MAGIC     *   Easiest to use.
# MAGIC
# MAGIC 2.  **External Tables (Unmanaged)**:
# MAGIC     *   You manage the data files in your own cloud storage (S3, ADLS, GCS or Volume).
# MAGIC     *   When you `DROP TABLE`, the metadata is removed, but the **files remain**.
# MAGIC     *   Requires an `External Location` to be configured in Unity Catalog.
# MAGIC
# MAGIC **note: always change the Serverless Environment version to version 4**

# COMMAND ----------

# Setup variables from previous lesson
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username_suffix = current_user.split("@")[0].replace(".", "_")
catalog_name = f"training_catalog_{username_suffix}"
# Set a widget or variable for SQL use
dbutils.widgets.text("catalog_name", catalog_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG IDENTIFIER(:catalog_name);
# MAGIC USE SCHEMA learning_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create a Managed Table
# MAGIC
# MAGIC This is the default. We don't specify a `LOCATION`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS managed_users (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   email STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO managed_users VALUES 
# MAGIC (1, 'Alice', 'alice@example.com'),
# MAGIC (2, 'Bob', 'bob@example.com');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM managed_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Check Table Details
# MAGIC
# MAGIC Look for the `Type` (should be MANAGED) and `Location` (should be a path managed by UC).

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: External Tables (Concept)
# MAGIC
# MAGIC To create an external table, you need a defined `EXTERNAL LOCATION`.
# MAGIC
# MAGIC Syntax:
# MAGIC ```sql
# MAGIC CREATE TABLE external_users (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )
# MAGIC LOCATION 's3://my-bucket/path/to/data';
# MAGIC ```
# MAGIC
# MAGIC *Note: Since we might not have a configured external location in this training environment, we will skip the execution of this step, but remember the difference!*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: CTAS (Create Table As Select)
# MAGIC
# MAGIC A very common pattern is creating a new table from a query.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS users_copy
# MAGIC AS SELECT * FROM managed_users;
# MAGIC
# MAGIC SELECT * FROM users_copy;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC 1. Drop the `users_copy` table.
# MAGIC 2. Verify that `managed_users` still exists.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Your code here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC We have tables. Now let's look at Views and Functions.
