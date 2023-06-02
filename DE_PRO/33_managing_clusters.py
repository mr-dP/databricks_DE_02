# Databricks notebook source
# How to manage cluster permissions and monitor its performance?

# COMMAND ----------

# You can configure 2 types of Cluster permissions.

# The first one is accessed from the "Admin Settings"
    # Under "Users" tab, you can create user ability to create clusters by enabling the "Allow unrestricted cluster creation" permission

# The second type is the cluster level permissions
    # Go to the "Compute" tab. From the clusters list, click the "more" meny and select "Edit permissions"
    # Cluster level permissions control user ability to use and modify the cluster
    # There are 3 permission levels for Databricks clusters:
        # "Can Attach To permission on cluster" give the ability too attach a notebook to the cluster and view the cluster metrics, logs and Spark UI
        # "Can Restart permission on cluster" gives all the previous level abilities in addition to start, terminate and restart the cluster
        # "Can Manage permission on clsuter" gives full permission on the cluster. In addition to all the previous level abilities, you can edit the cluster and its permission
    # You can assign these permission levels to Users or Group of users

# COMMAND ----------

# Databricks provide several options of logging cluster related activities

# Event log
    # Shows important life cyle events that happened with the cluster
    # This includes both events that are triggered manually by user actions or manually by Databricks
    # For example, when the cluster was created or terminated, if it is edited, or if it is running fine
    # This helps to track the activity of a cluster
    # You can filter the log by "Event Type"
    # An interesting Event Type is "Resizing". This allow you to review the timeline for a cluster auto-scaling events, both scaling up and scaling down

# Driver logs
    # Here, you get the logs generated within the cluster
    # All the direct print and log statements from your notebooks, hobs and libraries go to the Driver logs
    # These logs have 3 outputs - Standard output, Standard error and Log4j output
        # For example, from a notebook, if you try to print a statement, you will find the message in the Standard output
    # you can download the log file simply by clicking on the file name

# Metrics
    # From here, you can monitor the performance of your cluster
    # For this, Databricks provide access to the "Ganglia" metrics
    # To access the Ganglia UI, click on the "Ganglia UI" link
    # Ganglia UI provides you with overall cluster health. This helps you to understand what is going on on your cluster and its performance
    # There are 4 graphs that shows the usage of your cluster resources
        # The first graph shows the overall workload of your cluster in terms of the number of processes and nodes currently running
        # Overall memory usage. It shows how much RAM is being used for caching, processing and storing data across all the cluster nodes
        # In the CPU usage graph where you may see spikes that indicate how buzy your cluster is
        # The Network Usage
    # At the bottom of the page, you can see statistics per each node of the cluster

# COMMAND ----------


