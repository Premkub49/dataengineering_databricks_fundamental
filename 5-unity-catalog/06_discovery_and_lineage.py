# Databricks notebook source
# MAGIC %md
# MAGIC # Module 6: Discovery and Lineage
# MAGIC
# MAGIC **Objective:** Learn how to find and track data assets.
# MAGIC *   Use the **Lineage UI** to visualize data flow from source to downstream views.
# MAGIC *   Query the **Information Schema** to programmatically discover tables and columns.
# MAGIC *   Use the workspace search to find data assets by metadata.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC Unity Catalog automatically captures data lineage. This means it knows:
# MAGIC *   Which tables were used to create a view.
# MAGIC *   Which columns are derived from which upstream columns.
# MAGIC
# MAGIC ## Step 1: Visual Lineage
# MAGIC
# MAGIC 1.  Click on "Catalog" in the left sidebar.
# MAGIC 2.  Navigate to your catalog -> `learning_schema` -> `public_user_info` (the view we created).
# MAGIC 3.  Click the **Lineage** tab.
# MAGIC 4.  You should see a graph connecting `managed_users` -> `public_user_info`.

# COMMAND ----------

# Setup variables
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username_suffix = current_user.split("@")[0].replace(".", "_")
catalog_name = f"training_catalog_{username_suffix}"
dbutils.widgets.text("catalog_name", catalog_name)
print(catalog_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: System Tables (Information Schema)
# MAGIC
# MAGIC You can query metadata about your data using SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all tables in our schema
# MAGIC SELECT * FROM system.information_schema.tables
# MAGIC WHERE table_catalog = :catalog_name
# MAGIC AND table_schema = 'learning_schema';

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %sql
# MAGIC -- List all columns in our managed_users table
# MAGIC SELECT * FROM system.information_schema.columns
# MAGIC WHERE table_catalog = :catalog_name
# MAGIC AND table_schema = 'learning_schema'
# MAGIC AND table_name = 'managed_users';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Search
# MAGIC
# MAGIC You can use the search bar at the top of the Databricks workspace to find tables by name, column name, or comment.
# MAGIC
# MAGIC Try searching for "managed_users".
