# Databricks notebook source
# MAGIC %md
# MAGIC # Module 9: Table ACL and Fine-Grained Security
# MAGIC
# MAGIC **Objective:** Implement advanced security patterns.
# MAGIC *   Implement **Column-Level Masking** to hide sensitive data (PII).
# MAGIC *   Implement **Row-Level Security** to restrict data access based on user attributes (e.g., Region).
# MAGIC *   Compare **Dynamic Views** vs. **Native Row Filters & Column Masks**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC In this workshop, we will dive deep into securing your data at a granular level. We will cover Table Access Control Lists (ACLs) and the two main ways to implement Row and Column level security.
# MAGIC
# MAGIC ## Key Concepts
# MAGIC 1.  **Table ACLs**: Managing permissions (`SELECT`, `MODIFY`, etc.) on tables and views.
# MAGIC 2.  **Column-Level Security**: Restricting access to specific columns (e.g., masking PII).
# MAGIC 3.  **Row-Level Security**: Restricting access to specific rows (e.g., regional data access).
# MAGIC 4.  **Dynamic Views vs. Native Filters/Masks**: Two ways to implement fine-grained security.

# COMMAND ----------

# Setup variables
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username_suffix = current_user.split("@")[0].replace(".", "_")
catalog_name = f"training_catalog_{username_suffix}"
dbutils.widgets.text("catalog_name", catalog_name)
print(catalog_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG :catalog_name;
# MAGIC USE SCHEMA learning_schema;
# MAGIC
# MAGIC -- Let's ensure we have some data to work with
# MAGIC CREATE TABLE IF NOT EXISTS sensitive_employee_data (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   salary INT,
# MAGIC   region STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO sensitive_employee_data VALUES 
# MAGIC (1, 'Alice', 'alice@company.com', 100000, 'North'),
# MAGIC (2, 'Bob', 'bob@company.com', 90000, 'South'),
# MAGIC (3, 'Charlie', 'charlie@company.com', 110000, 'North'),
# MAGIC (4, 'David', 'david@company.com', 85000, 'West');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Table ACLs (Review)
# MAGIC
# MAGIC Permissions in Unity Catalog are inherited. If you have `SELECT` on a Schema, you have `SELECT` on all tables in that schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Granting read-only access to a specific table
# MAGIC -- GRANT SELECT ON TABLE sensitive_employee_data TO `account users`;
# MAGIC
# MAGIC -- Checking current permissions
# MAGIC SHOW GRANTS ON TABLE sensitive_employee_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Column-Level Security (using Dynamic Views)
# MAGIC
# MAGIC We want everyone to see names, but only the `admin` group to see the `salary`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW employee_public_view AS
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   name,
# MAGIC   region,
# MAGIC   CASE 
# MAGIC     WHEN is_account_group_member('admin') THEN salary 
# MAGIC     ELSE NULL -- Masking the salary for non-HR
# MAGIC   END AS salary
# MAGIC FROM sensitive_employee_data;
# MAGIC
# MAGIC -- Test the view
# MAGIC SELECT * FROM employee_public_view;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Row-Level Security (using Dynamic Views)
# MAGIC
# MAGIC We want users to only see employees from their own region. (For this demo, we'll simulate a 'North' region user).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW regional_employee_view AS
# MAGIC SELECT * FROM sensitive_employee_data
# MAGIC WHERE 
# MAGIC   is_account_group_member('admin') -- Admins see everything
# MAGIC   OR (region = 'North'); -- Simulated: In reality, you'd use a mapping table or group name
# MAGIC
# MAGIC -- Test the view
# MAGIC SELECT * FROM regional_employee_view;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Native Row Filters and Column Masks (Modern Way)
# MAGIC
# MAGIC Unity Catalog now supports native filters and masks directly on the table, which is more performant and easier to manage than views.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Column Masking
# MAGIC First, we create a function that defines the masking logic.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION salary_mask(salary INT)
# MAGIC RETURN CASE 
# MAGIC   WHEN is_account_group_member('hr_admins') THEN salary 
# MAGIC   ELSE -1 -- Return -1 for unauthorized users
# MAGIC END;
# MAGIC
# MAGIC -- Apply the mask to the table
# MAGIC ALTER TABLE sensitive_employee_data ALTER COLUMN salary SET MASK salary_mask;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Row Filtering
# MAGIC We create a function that returns a boolean.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION region_filter(region STRING)
# MAGIC RETURN is_account_group_member('admin') OR region = 'North';
# MAGIC
# MAGIC -- Apply the filter to the table
# MAGIC ALTER TABLE sensitive_employee_data SET ROW FILTER region_filter ON (region);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Fine-Grained Security
# MAGIC
# MAGIC Now, when you query the **base table** directly, the security is applied automatically!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Even though we query the table, the salary will be -1 and rows will be filtered
# MAGIC -- (Unless you are an admin/HR)
# MAGIC SELECT * FROM sensitive_employee_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC 1. Create a column mask for the `email` column that only shows the domain (e.g., `****@company.com`) for non-admins.
# MAGIC 2. Remove the row filter from the table: `ALTER TABLE sensitive_employee_data DROP ROW FILTER;`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Your code here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC You have now mastered:
# MAGIC 1.  Basic Table ACLs.
# MAGIC 2.  Implementing Row/Column security using **Dynamic Views**.
# MAGIC 3.  Implementing Row/Column security using **Native Filters and Masks**.
