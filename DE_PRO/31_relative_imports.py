# Databricks notebook source
# We are going to see how Relative Imports can ve used instead of the "%run" magic command
# The "%run" magic command is used to run another notebook from the current notebook

# COMMAND ----------

# MAGIC %run ./helper/cube_notebook

# COMMAND ----------

# All local objects of the referenced notebook, such as function, variables and classes, will be available here in this notebook

# COMMAND ----------

# Lets create a cube and calculate its volume

c1 = Cube(3)
c1.get_volume()

# COMMAND ----------

# Since we are using Python, wouldn't it make more sense if we could use something like this for importing our "Cube" class
from helper.cube_notebook import Cube

# ModuleNotFoundError: No module named 'helper.cube_notebook'

# this does not work with Python notebooks
# Instead your code need to be in a Python file with ".py" extension

# COMMAND ----------

# In Databricks Repos, in addition to notebooks, we can create arbitrary files

# Go to Admin Settings -> Workspace settings -> Repos -> Files in Repos: DBR 8.4+

# Create -> File -> Name is 'cube.py'

# COMMAND ----------

# Now we can simply type

from helper.cube import Cube_PY

# COMMAND ----------

# While the content of the cube_notebook and cube.py may look identical, if you look at the cube_notebook in Github, you see a special comment "#Databricks notebook source"
    # This is an important comment since it establishes this python file as a notebook

# COMMAND ----------

# Lets confirm the import by creating a new instance of the class Cube_PY

c2 = Cube_PY(3)
c2.get_volume()

# COMMAND ----------

# This worked well because the "helper" subdirectory resides in the current working directory of our notebook

# COMMAND ----------

# The "%sh" magic command allows you to run shell code in your notebook

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

# MAGIC %sh ls ./helper

# COMMAND ----------

# The current working working directory is included in the PYTHON PATH

# COMMAND ----------

# If you want to import modules from outside this directory, you will need to first append it to the "sys.path" variable

# COMMAND ----------

import sys

for path in sys.path:
    print(path)

# "sys.path" is a list of directories where Python interpreter searches for modules

# COMMAND ----------

# We can use the method "sys.path.append()" to add our modules directory to the path variable

import os

sys.path.append(os.path.abspath("../modules"))

# COMMAND ----------

import sys

for path in sys.path:
    print(path)

# COMMAND ----------

# Now we can import the "Cube" class from the "shapes" package found in the "modules" directory

from shapes.cube import Cube as CubeShape

# COMMAND ----------

c3 = CubeShape(3)
c3.get_volume()

# COMMAND ----------

# In Databricks Notebooks, you can also install Python Wheels
# Python Wheels are pre-built binary package formats for Python modules and libraries
# To install a python wheel, we use the "%pip" magic command
# "%pip install" allow us to install python wheels scoped to the current notebook session

# The python interpreter will be restarted after running this command. This clears all the variables declared in this notebook
    # This is why we should place all the "pip" commands at the beginning of our notebooks

# COMMAND ----------

# MAGIC %pip install ../wheels/shapes-1.0.0-py3-none-any.whl

# COMMAND ----------

# Now with our wheel installed, we can import our "Cube" class coming from this wheel

from shapes_wheel.cube import Cube as Cube_WHL

# COMMAND ----------

c4 = Cube_WHL(3)
c4.get_volume()

# COMMAND ----------

# Why we did not use the "%sh" magic command for running the "pip install" command?
#               %sh pip install ../wheels/shapes-1.0.0-py3-none-any.whl

# The "%sh" magic command executes shell code only on the local driver machine, while with the "%pip" magic, Python packages will be installed on all the nodes on the currently active cluster

# COMMAND ----------


