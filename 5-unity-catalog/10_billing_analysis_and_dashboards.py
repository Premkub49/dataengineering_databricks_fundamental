# Databricks notebook source
# MAGIC %md
# MAGIC # Module 10: Bonus - Billing Analysis and Dashboards
# MAGIC
# MAGIC **Objective:** Use data to manage Databricks costs.
# MAGIC *   Query the `system.billing.usage` table to track DBU consumption.
# MAGIC *   Aggregate costs by Date, SKU, and Workspace.
# MAGIC *   Learn how to build a visual **Cost Dashboard** in Databricks SQL.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC In this bonus module, we will explore how to use Unity Catalog **System Tables** to analyze your Databricks consumption (DBUs) and build a cost-tracking dashboard.
# MAGIC
# MAGIC ## Key Concepts
# MAGIC 1.  **Billing System Tables**: Tables in `system.billing` that track DBU usage.
# MAGIC 2.  **Aggregation**: Grouping costs by date, workspace, or SKU.
# MAGIC 3.  **Dashboards**: Visualizing usage trends to prevent "bill shock".
# MAGIC
# MAGIC ### Prerequisites
# MAGIC *   Unity Catalog must be enabled.
# MAGIC *   System tables must be enabled by an account admin.
# MAGIC *   You need `SELECT` permissions on `system.billing.usage`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Explore the Billing Table
# MAGIC
# MAGIC The primary table for billing is `system.billing.usage`. Let's see what columns are available.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview the billing data
# MAGIC SELECT * FROM system.billing.usage LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Aggregate Daily Usage
# MAGIC
# MAGIC Let's create a query that calculates the total DBUs consumed per day for the last 30 days.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   usage_date,
# MAGIC   sum(usage_quantity) as total_dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date > current_date() - interval 30 days
# MAGIC GROUP BY usage_date
# MAGIC ORDER BY usage_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create an Aggregate Billing View
# MAGIC
# MAGIC To make dashboarding easier, we will create a view that breaks down usage by **SKU** (e.g., All Purpose Compute, Jobs Compute, SQL Pro).

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ""; -- TODO add catalog name here
# MAGIC CREATE SCHEMA IF NOT EXISTS billing_analysis;
# MAGIC USE SCHEMA billing_analysis;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW daily_usage_by_sku AS
# MAGIC SELECT 
# MAGIC   usage_date,
# MAGIC   sku_name,
# MAGIC   sum(usage_quantity) as total_dbus
# MAGIC FROM system.billing.usage
# MAGIC GROUP BY ALL;
# MAGIC
# MAGIC SELECT * FROM daily_usage_by_sku;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Building the Dashboard
# MAGIC
# MAGIC Now that we have our aggregate view, follow these steps to create a dashboard:
# MAGIC
# MAGIC 1.  **Open Databricks SQL**: Click the "SQL" persona in the left sidebar.
# MAGIC 2.  **Create a New Query**:
# MAGIC     *   Paste: `SELECT * FROM {your_catalog_name}.billing_analysis.daily_usage_by_sku`
# MAGIC     *   Run the query and save it as "Daily DBU Usage".
# MAGIC 3.  **Add Visualization**:
# MAGIC     *   Click "Add Visualization".
# MAGIC     *   Select **Area Chart**.
# MAGIC     *   X-Axis: `usage_date`.
# MAGIC     *   Y-Axis: `total_dbus`.
# MAGIC     *   Grouping: `sku_name`.
# MAGIC 4.  **Create Dashboard**:
# MAGIC     *   Click "Dashboards" in the sidebar -> "Create Dashboard".
# MAGIC     *   Add the visualization you just created.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC Congratulations! You've completed the entire Unity Catalog course, including the bonus billing module. You now have the tools to govern data, secure it, and even manage the costs associated with it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion
# MAGIC
# MAGIC Congratulations! You have completed the Introduction to Unity Catalog course.
# MAGIC
# MAGIC You have learned:
# MAGIC 1.  **The 3-level namespace**: `catalog.schema.table`.
# MAGIC 2.  **Resource Management**: Creating Catalogs, Schemas, and Tables.
# MAGIC 3.  **Managed vs. External**: Understanding data ownership.
# MAGIC 4.  **Views and Functions**: Encapsulating logic and securing data.
# MAGIC 5.  **Governance**: Granting/Revoking privileges on Tables, Volumes, and Functions.
# MAGIC 6.  **Ownership**: Managing object owners and transfers.
# MAGIC 7.  **Discovery**: Using Lineage and Information Schema.
# MAGIC 8.  **Delta Sharing**: Securely sharing data outside your workspace.
# MAGIC 9.  **Audit Logs**: Tracking "who did what" using the `system` catalog.
# MAGIC 10. **Fine-Grained Security**: Implementing Table ACLs, Row Filters, and Column Masks.
# MAGIC 11. **Billing Analysis**: Tracking and visualizing costs using system tables.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Clean Up
# MAGIC Run the cell below to clean up the resources created in this course.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP CATALOG IF EXISTS ${catalog_name} CASCADE;
