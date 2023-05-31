# Databricks notebook source
# Dynamic Views allow identify ACL or Access Control List to be applied to data in a table at the column or row level
# So, users with sufficient privilege will be able to see all the fields, while restricted users will be shown arbitrary results as defined at view creation

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE customers_silver;
# MAGIC
# MAGIC -- Fields like email, first_name, last_name and street address are problamatic since they allow to identify the customer. So, let use try to redact these fields

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW customers_vw AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   CASE
# MAGIC     WHEN is_member('admins_demo') THEN email
# MAGIC     ELSE 'REDACTED'
# MAGIC   END AS email,
# MAGIC   gender,
# MAGIC   CASE
# MAGIC     WHEN is_member('admins_demo') THEN first_name
# MAGIC     ELSE 'REDACTED'
# MAGIC   END AS first_name,
# MAGIC   CASE
# MAGIC     WHEN is_member('admins_demo') THEN last_name
# MAGIC     ELSE 'REDACTED'
# MAGIC   END AS last_name,
# MAGIC   CASE
# MAGIC     WHEN is_member('admins_demo') THEN street
# MAGIC     ELSE 'REDACTED'
# MAGIC   END AS street,
# MAGIC   city,
# MAGIC   country,
# MAGIC   row_time
# MAGIC FROM
# MAGIC   customers_silver;
# MAGIC
# MAGIC -- Here we create a redacted view to hide this information from unauthorized users at the column level
# MAGIC -- We use the "is_member()" function, so only members of the group "admins_demo" will be able to see the results in a plain text

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM customers_vw;
# MAGIC
# MAGIC -- Currently, we are not a member of the "admins_demo" group

# COMMAND ----------

# Go to "Admin Settings" -> Create a Group and name it "admins_demo" -> Add my user to the Group

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM customers_vw;
# MAGIC
# MAGIC -- I am now authorized to see the results in a plain text

# COMMAND ----------

# Let us apply this logic also at the row level

# COMMAND ----------

# MAGIC %sql
# MAGIC -- For row-level access control, we can add WHERE clauses to filter source data on different conditions
# MAGIC -- Here all the members of the group "admins_demo" will have full access to the underlying data since the WHERE condition will be "TRUE" for every record
# MAGIC -- On the other hand, users that are not members of the specified group will only be able to see records for "France" that have been updated after the specified date
# MAGIC -- Views can be layered on top of one another. Here, the "customer_vw" from the previous step is modified with conditional row access
# MAGIC
# MAGIC CREATE
# MAGIC OR REPLACE VIEW customers_fr_vw AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customers_vw
# MAGIC WHERE
# MAGIC   CASE
# MAGIC     WHEN is_member('admins_demo') THEN TRUE
# MAGIC     ELSE country = "France"
# MAGIC     AND row_time > "2022-01-01"
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now lets query this view as a member of the "admins_demo" group
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customers_fr_vw;

# COMMAND ----------

# Let us remove my user from the "admins_demo" group

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customers_fr_vw;
# MAGIC
# MAGIC -- As expected, the fields are redacted and in addition I see only "France" records updates after the specified date

# COMMAND ----------


