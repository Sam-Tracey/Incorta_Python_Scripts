'''
Author: Sam Tracey.
Date: 2022-11-10
Topic: This script will list all the installed packages in the cluster and convert the list to a spark dataframe.

'''

# Importing the required libraries
import pkg_resources
from pyspark.sql.types import StringType

# Creating a list of all the installed packages
installed_packages = pkg_resources.working_set

# Looping through the list of installed packages and creating a list of the package names and version.
installed_packages_list = sorted(["%s==%s" % (i.key, i.version)
     for i in installed_packages])

# Converting the list to a spark dataframe
df = spark.createDataFrame(installed_packages_list, StringType())

# Save the dataframe to a materialized view.
save(df)