# Databricks notebook source
# There are 2 categories of tests for data pipelines:
    # Data Quality Tests
    # Standard Tests

# COMMAND ----------

# Data Quality Tests
# ------------------

# Data Quality Tests are used to test the quality of the data
    # For example, you can test that a column "price" has only values greater than 0
# We can ensure quality of our data by applying CHECK constraints to Delta Tables

# COMMAND ----------

# Standard Tests
# --------------

# Standard Tests are used to test the code logic
    # These tests are run every time your code is modified
# There are 3 common types of Standard Tests
    # Unit Testing
    # Integration Testing
    # End-to-end Testing

# COMMAND ----------

# Unit Testing
# ------------

# Unit Testing is an approach of testing units of code such as functions
# So if you make any change to them in the future, you can use Unit Tests to determine whether they still work as ypu expect them to
# This allows you to find problem with your code faster and earlier in the development life cycle

# COMMAND ----------

# Unit Tests are done using Assertions
# An Assertion is a statement that enables you to test the assumption you have made in your code
# It is simple to use by adding the "assert" keyword followed by a boolean condition
# For example, to verify if a function call returns an expected value
#                   assert func() == expected_value
# With Assertions you check if your assumptions remain true while you are developing your code

# COMMAND ----------

# Integration Testing
# -------------------

# Although each module is unit tested, we need another type of testing where software modules are integrated logically and tested as a group. For this, we use Integration Testing.
# With Integration Testing, we validate the interaction between the sub-systems of our application.

# COMMAND ----------

# End-to-end Testing
# ------------------

# It ensures that your application can run properly under real-world scenarios.
# The goal of this testing is to closely simulate a user experience from start to finish
