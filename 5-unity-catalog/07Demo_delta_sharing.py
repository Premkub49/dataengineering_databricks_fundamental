# Databricks notebook source
# MAGIC %md
# MAGIC # Module 7: Delta Sharing
# MAGIC
# MAGIC **Objective:** Understand secure data sharing without data copying.
# MAGIC *   Learn the Delta Sharing architecture (Provider, Share, Recipient).
# MAGIC *   Create **Shares** and add tables to them.
# MAGIC *   Manage **Recipients** and grant them access to specific shares.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC Delta Sharing is an open protocol developed by Databricks for secure data sharing with other organizations, or other departments within your organization, regardless of which computing platforms they use.
# MAGIC
# MAGIC ## Key Concepts
# MAGIC 1.  **Share**: A logical container for the tables you want to share.
# MAGIC 2.  **Recipient**: An entity that receives the data (can be a Databricks user or an external user).
# MAGIC 3.  **Provider**: The organization sharing the data.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC *   Unity Catalog must be enabled.
# MAGIC *   You need `CREATE SHARE` and `CREATE RECIPIENT` privileges (usually admin).

# COMMAND ----------

# Setup variables
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username_suffix = current_user.split("@")[0].replace(".", "_")
catalog_name = f"training_catalog_{username_suffix}"
dbutils.widgets.text("catalog_name", catalog_name)
print(catalog_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create a Share
# MAGIC
# MAGIC A share is a collection of tables.
# MAGIC
# MAGIC First enable **External delta sharing**. on catalog share (upper top within metastore).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GRANT USE_SCHEMA, SELECT, EXECUTE, READ_VOLUME
# MAGIC ON CATALOG ${catalog_name}
# MAGIC TO `admin`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a share
# MAGIC CREATE SHARE IF NOT EXISTS my_training_share;
# MAGIC
# MAGIC -- Describe the share
# MAGIC DESCRIBE SHARE my_training_share;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Add Tables to the Share
# MAGIC
# MAGIC You can add tables from any catalog/schema you have access to.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add our managed_users table to the share
# MAGIC ALTER SHARE my_training_share ADD TABLE ${catalog_name}.learning_schema.managed_users;
# MAGIC
# MAGIC -- You can also provide an alias so the recipient sees a different name
# MAGIC -- ALTER SHARE my_training_share ADD TABLE ${catalog_name}.learning_schema.managed_users AS shared_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create a Recipient
# MAGIC
# MAGIC There are two types of recipients:
# MAGIC 1.  **Databricks Recipient**: For sharing between Databricks workspaces.
# MAGIC 2.  **Token-based Recipient**: For sharing with non-Databricks users (they get a download link for a credential file).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a recipient (Databricks-to-Databricks example)
# MAGIC -- In a real scenario, you'd use the sharing identifier of the target workspace
# MAGIC CREATE RECIPIENT IF NOT EXISTS training_recipient 
# MAGIC COMMENT 'Recipient for Unity Catalog training';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Grant Access to the Share
# MAGIC
# MAGIC Now we connect the Recipient to the Share.

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON SHARE my_training_share TO RECIPIENT training_recipient;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: How the Recipient Sees the Data
# MAGIC
# MAGIC If you were the recipient, you would see the share as a "Provider" and you would create a catalog from it.
# MAGIC
# MAGIC ```sql
# MAGIC -- This is what the RECIPIENT would run:
# MAGIC -- CREATE CATALOG shared_data_catalog USING SHARE provider_name.my_training_share;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC 1. List all shares you have created: `SHOW SHARES;`
# MAGIC 2. List all recipients: `SHOW RECIPIENTS;`
# MAGIC 3. See what is inside your share: `DESCRIBE SHARE my_training_share;`

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SHARES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC This concludes the Delta Sharing introduction. In a real-world scenario, you would manage these through the "Data Sharing" menu in the Databricks UI for a more visual experience.
