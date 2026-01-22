# Databricks notebook source
# MAGIC %md
# MAGIC # Module 2: Catalogs and Schemas
# MAGIC
# MAGIC **Objective:** Master the creation and organization of data containers.
# MAGIC *   Learn how to create and describe **Catalogs**.
# MAGIC *   Learn how to create **Schemas** (Databases) within a catalog.
# MAGIC *   Understand how to set the current context using the `USE` command.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC In this module, we will learn how to create the containers for our data.
# MAGIC
# MAGIC ## The Hierarchy
# MAGIC 1.  **Metastore**: The top-level container (usually managed by admins and only be use with premium and above).
# MAGIC 2.  **Catalog**: The first level of the namespace. Used to group data assets (e.g., `prod`, `dev`, `hr_data`).
# MAGIC 3.  **Schema**: The second level (also known as Database). Used to organize tables and views.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create a Catalog
# MAGIC
# MAGIC Let's try to create a catalog named `training_catalog`. We will add a suffix with your username to keep it unique if many people are running this.
# MAGIC **note: always change the Serverless Environment version to version 4**

# COMMAND ----------

# Get current username to make resources unique
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username_suffix = current_user.split("@")[0].replace(".", "_")
catalog_name = f"training_catalog_{username_suffix}"

print(f"We will use catalog name: {catalog_name}")

# Set a widget or variable for SQL use
dbutils.widgets.text("catalog_name", catalog_name)

# COMMAND ----------

# DBTITLE 1,Cell 4
#Create the catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
#Describe it to see details
spark.sql(f"DESCRIBE CATALOG {catalog_name}").display()
#If you see an error, make sure the 'catalog_name' parameter is set to a valid value above.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Select the Catalog
# MAGIC
# MAGIC Now we set this as our current context.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG IDENTIFIER(:catalog_name);
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create a Schema
# MAGIC
# MAGIC Inside the catalog, we create a schema (database).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog_name || '.' || 'learning_schema')
# MAGIC COMMENT 'A schema for learning Unity Catalog';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Explore
# MAGIC
# MAGIC Let's verify what we created.

# COMMAND ----------

#Show schemas in our new catalog
spark.sql(f"SHOW SCHEMAS IN {catalog_name}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC 1. Create a second schema named `sandbox_schema`.
# MAGIC 2. Drop the `sandbox_schema` using `DROP SCHEMA`.

# COMMAND ----------

# DBTITLE 1,TO DO
# MAGIC %sql
# MAGIC -- Your code here for the exercise

# COMMAND ----------

# DBTITLE 1,answer 1
# MAGIC %skip
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS sandbox_schema;

# COMMAND ----------

# DBTITLE 1,answer 2
# MAGIC %skip
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC DROP SCHEMA sandbox_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC Now that we have a home for our data (`catalog.schema`), let's create some tables in the next module!
