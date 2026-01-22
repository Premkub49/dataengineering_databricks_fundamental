# Databricks notebook source
# MAGIC %md
# MAGIC # Module 8: Unity Catalog Audit Logs
# MAGIC
# MAGIC **Objective:** Learn to monitor workspace activity for security and compliance.
# MAGIC *   Access and query the `system.access.audit` table.
# MAGIC *   Identify "who did what and when" (e.g., who accessed a sensitive table).
# MAGIC *   Track permission changes (Grants/Revokes) across the account.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC Unity Catalog provides a centralized audit log for all actions performed on data and AI assets. These logs are stored in the `system` catalog, which is a special catalog that contains metadata and operational data for your account.
# MAGIC
# MAGIC ## Key Concepts
# MAGIC 1.  **System Catalog**: A built-in catalog named `system` that provides visibility into account-level activities.
# MAGIC 2.  **Audit Logs**: Records of "who did what and when" across the workspace.
# MAGIC 3.  **Information Schema**: Metadata about tables, columns, and permissions (which we touched on in Module 6).
# MAGIC
# MAGIC ### Prerequisites
# MAGIC *   Unity Catalog must be enabled.
# MAGIC *   You need access to the `system` catalog (usually granted to admins or via specific grants).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Explore the System Catalog
# MAGIC
# MAGIC Let's see what's inside the `system` catalog. The audit logs are typically found in `system.access.audit`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List schemas in the system catalog
# MAGIC SHOW SCHEMAS IN system;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Querying Audit Logs
# MAGIC
# MAGIC The `system.access.audit` table contains records of all events. Let's look at the most recent events.
# MAGIC
# MAGIC *Note: If the `system` catalog is not enabled or you don't have permissions, this cell might fail. In a real production environment, an admin must enable system tables.*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the latest 10 audit log entries
# MAGIC SELECT 
# MAGIC   event_time, 
# MAGIC   user_identity.email as user, 
# MAGIC   service_name, 
# MAGIC   action_name, 
# MAGIC   request_params
# MAGIC FROM system.access.audit
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Common Audit Scenarios
# MAGIC
# MAGIC ### Scenario A: Who accessed a specific table?
# MAGIC This is crucial for compliance and security.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find who accessed a specific table in the last 24 hours
# MAGIC SELECT 
# MAGIC   event_time, 
# MAGIC   user_identity.email, 
# MAGIC   action_name
# MAGIC FROM system.access.audit
# MAGIC WHERE request_params.table_full_name = '' -- Change this to your table
# MAGIC AND event_time > current_timestamp() - interval 24 hours
# MAGIC ORDER BY event_time DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Visualizing Audit Data
# MAGIC
# MAGIC You can use Databricks SQL or Notebook visualizations to create dashboards from these logs. 
# MAGIC
# MAGIC **Example Chart Ideas:**
# MAGIC *   Top users by activity.
# MAGIC *   Most frequently accessed tables.
# MAGIC *   Failed access attempts (Security monitoring).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC 1. Query the audit logs to find your own activities from today.
# MAGIC 2. Filter the logs to see if anyone has tried to `DROP` a table in the last week.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Your code here for the exercise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC Audit logs are the foundation of data security and compliance. In the next steps, you might explore **Predictive Optimization** or **Billing Logs** also found in the `system` catalog.
