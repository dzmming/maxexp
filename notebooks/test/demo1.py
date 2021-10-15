# Databricks notebook source
# MAGIC %fs ls /FileStore

# COMMAND ----------

#dbutils.fs.fsutils.head('/init-scripts/datadog-install-driver-only.sh')
# dbutils.fs.mkdirs("dbfs:/databricks/scripts/")

display(dbutils.fs.ls("dbfs:/init-scripts/"))


# COMMAND ----------

from AirfowUtils import DatabricksGitService
from AirfowUtils import PubUdf

databricks_host = "https://adb-4237834302559111.11.azuredatabricks.net"
databricks_token = 'dapi33941b155e5b13c240453935528d01ed-3'
repo_url = "https://github.com/dzmming/maxexp"
repo_path = "/Repos/test1/maxexp"
branch = "master"

dg = DatabricksGitService.DatabricksGitService(databricks_host, databricks_token, repo_url, repo_path, branch)

print(PubUdf.getNow())


# COMMAND ----------

# import numpy
# import click
# import pyutil

# data = [1,2,3,4,5,6]
# x = numpy.array(data)
# print(x)

# click.echo('Initialized the cluster')



# COMMAND ----------

# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": "10573df0-f24a-4f83-9c1d-7b980dda5b64",
#           "fs.azure.account.oauth2.client.secret": "W675gdU~6xeP7.F7dJ-E8Nc~1RB.gQ2Q6Y",
#           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

# # dbutils.fs.unmount("/mnt/daxadsl")
# dbutils.fs.mount(
#   source = "abfss://dax@mylakestorage.dfs.core.windows.net/",
#   mount_point = "/mnt/daxadsl",
#   extra_configs = configs)

# dbutils.secrets.get(scope="zha-akv-scope",key="zha-adls2-secret")

# COMMAND ----------

# blobdemokey = dbutils.secrets.get(scope = "zhasecretsscope", key = "zhablobdemo")
# print(blobdemokey)

# # print(dbutils.secrets.get("zhasecretsscope","zhablobdemo"))
# #dbutils.fs.unmount("/mnt/daxblob")

# dbutils.fs.mount(
#   source = "wasbs://test2@dzmstorage.blob.core.windows.net",
#   mount_point = "/mnt/daxblob",
#   extra_configs = {"fs.azure.account.key.dzmstorage.blob.core.windows.net":dbutils.secrets.get(scope = "zhasecretsscope", key = "zhablobdemo")})



# COMMAND ----------

#dbutils.fs.unmount("/mnt/daxadsl")
dbutils.fs.ls("/mnt/daxadsl/csv")

# COMMAND ----------


df = spark.read \
  .option("header", "true") \
  .option("inferSchema", True) \
  .csv("/mnt/daxadsl/csv")  \
  
display(df)


df_cars = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/daxadsl/csv')
display(df_cars)


# COMMAND ----------

dbutils.fs.cp('/mnt/daxadsl/csv/UsedCars.csv', '/mnt/daxadsl/csv/UsedCars2.csv')

#dbutils.fs.cp('C:\\Users\\zhadeng\\deng\msWork\\Airflow\\process_databricks_demo.py', '/mnt/daxadsl/2.py')


#hdfs dfs –ls wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/
#hdfs dfs -ls abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/

#hadoop distcp wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/example/data/gutenberg abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/myfolder

#azcopy copy "C:\Users\zhadeng\deng\msWork\Airflow\process_databricks_demo.py"  "https://mylakestorage.blob.core.windows.net/test2/33.py?"

#?[sas-token]

#azcopy copy "C:\local\path" "https://account.blob.core.windows.net/mycontainer1/?sv=2018-03-28&ss=bjqt&srt=sco&sp=rwddgcup&se=2019-05-01T05:01:17Z&st=2019-04-30T21:01:17Z&spr=https&sig=MGCXiyEzbtttkr3ewJIh2AR8KrghSy1DGM9ovN734bQF4%3D" --recursive=true


# COMMAND ----------

from shutil import copyfile

with open("/dbfs/tmp/test_dbfs.txt", 'w') as f:
  f.write("Apache Spark is awesome!\n")
  f.write("End of example!")
  
  
#copyfile('/home/deng/ddd.txt', '/dbfs/tmp/demo3.txt')
