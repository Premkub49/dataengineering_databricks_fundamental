# Databricks notebook source
# MAGIC %md
# MAGIC # Module 5: Governance and Privileges
# MAGIC
# MAGIC **Objective:** Master the security model of Unity Catalog.
# MAGIC *   Learn the hierarchy of privileges (`USE CATALOG` -> `USE SCHEMA` -> `SELECT`).
# MAGIC *   Master `GRANT` and `REVOKE` commands for Tables, **Volumes**, and **Functions**.
# MAGIC *   Understand **Ownership** and how to transfer it between users or groups.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC Unity Catalog uses standard SQL `GRANT` and `REVOKE` syntax to manage permissions.
# MAGIC
# MAGIC Common privileges:
# MAGIC *   `USE CATALOG`: Required to traverse the catalog.
# MAGIC *   `USE SCHEMA`: Required to traverse the schema.
# MAGIC *   `SELECT`: Read data from a table or view.
# MAGIC *   `MODIFY`: Update/Delete/Insert data.
# MAGIC *   `ALL PRIVILEGES`: Full control.
# MAGIC
# MAGIC **note: always change the Serverless Environment version to version 4**
# MAGIC
# MAGIC
# MAGIC **This workshop will need to have another account to do the workshop**

# COMMAND ----------

# Setup variables
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username_suffix = current_user.split("@")[0].replace(".", "_")
catalog_name = f"training_catalog_{username_suffix}"
dbutils.widgets.text("catalog_name", catalog_name)
print(catalog_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG IDENTIFIER(:catalog_name);
# MAGIC USE SCHEMA learning_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Granting Privileges
# MAGIC To add a user to the workspace:
# MAGIC 1.  Go to the Databricks admin console (at your profile icon upper right side and click settings).
# MAGIC 2.  Click **Identity and access** > **Manage** > **Add User**.
# MAGIC 3.  Click **Add** to invite the user.
# MAGIC
# MAGIC To create a group of users:
# MAGIC 1.  In the admin console, navigate to **Groups**.
# MAGIC 2.  Click **Create Group**.
# MAGIC 3.  Enter a group name (e.g., `data_analysts`).
# MAGIC 4.  Add users to the group by selecting their email addresses.
# MAGIC 5.  Save the group.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Granting Privileges
# MAGIC
# MAGIC You can grant privileges to:
# MAGIC 1.  **Users**: Identified by email (e.g., `user@example.com`).
# MAGIC 2.  **Groups**: Identified by group name (e.g., `account users`, `data_analysts`).
# MAGIC 3.  **Service Principals**: For automated jobs.
# MAGIC
# MAGIC ### Hierarchy of Privileges
# MAGIC To access a table, a user needs:
# MAGIC 1.  `USE CATALOG` on the catalog.
# MAGIC 2.  `USE SCHEMA` on the schema.
# MAGIC 3.  `SELECT` (or other) on the table.

# COMMAND ----------

# DBTITLE 1,grant
# MAGIC %sql
# MAGIC -- Example 1: Grant basic traversal permissions to data_analysts users
# MAGIC
# MAGIC GRANT USE CATALOG ON CATALOG ${catalog_name} TO `data_analysts`;
# MAGIC GRANT USE SCHEMA ON SCHEMA learning_schema TO `data_analysts`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 2: Grant read access on a specific table to a specific user or group
# MAGIC -- Replace 'student@example.com' with a real email or group to test
# MAGIC GRANT SELECT ON TABLE managed_users TO `data_analysts`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example 3: Grant full control to a team
# MAGIC -- Note: ALL PRIVILEGES does not include the EXTERNAL USE SCHEMA, EXTERNAL USE LOCATION, or MANAGE privileges
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA learning_schema TO `data_analysts`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Revoking Privileges
# MAGIC
# MAGIC If you made a mistake or access needs change, you can revoke permissions.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revoke the previously granted permissions
# MAGIC REVOKE USE CATALOG ON CATALOG ${catalog_name} FROM `data_analysts`;
# MAGIC REVOKE USE SCHEMA ON SCHEMA learning_schema FROM `data_analysts`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Granting on Volumes
# MAGIC
# MAGIC Volumes are Unity Catalog objects that represent a logical volume of files in a cloud object storage location.
# MAGIC
# MAGIC *   `READ VOLUME`: Read files.
# MAGIC *   `WRITE VOLUME`: Upload/Delete files.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a volume first
# MAGIC CREATE VOLUME IF NOT EXISTS learning_schema.training_files;
# MAGIC
# MAGIC -- Grant read access
# MAGIC GRANT READ VOLUME ON VOLUME learning_schema.training_files TO `data_analysts`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Granting on Functions
# MAGIC
# MAGIC You can control who can execute the custom functions we created in Module 4.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant execute permission on our mask_email function
# MAGIC GRANT EXECUTE ON FUNCTION learning_schema.mask_email TO `data_analysts`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Ownership and Transfer
# MAGIC
# MAGIC Every object in Unity Catalog has an **Owner**. Only the owner (or an admin) can grant permissions on that object.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See who owns the table
# MAGIC DESCRIBE TABLE EXTENDED managed_users;
# MAGIC
# MAGIC -- Example: Transfer ownership to a different group
# MAGIC -- ALTER TABLE managed_users SET OWNER TO `data_engineers`;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Show Grants
# MAGIC
# MAGIC You can check who has access to an object.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE managed_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Dynamic Views (Row/Column Level Security)
# MAGIC
# MAGIC You can use functions like `is_account_group_member()` or `current_user()` to filter data dynamically.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW secure_users_view AS
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   name,
# MAGIC   CASE 
# MAGIC     WHEN is_account_group_member('admin') THEN email
# MAGIC     ELSE 'REDACTED'
# MAGIC   END AS email
# MAGIC FROM managed_users;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Since you are the owner/admin, you likely see the email.
# MAGIC -- Others would see 'REDACTED'.
# MAGIC SELECT * FROM secure_users_view;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC 1. Run `SHOW GRANTS` on your catalog.
# MAGIC 2. Try to `REVOKE` a privilege (if you granted one).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Your code here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC Finally, let's see how Unity Catalog helps us understand our data through Lineage.
