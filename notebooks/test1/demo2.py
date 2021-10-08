# Databricks notebook source
# MAGIC %fs ls /test

# COMMAND ----------

#dbutils.fs.unmount("/mnt/daxadsl")
dbutils.fs.ls("/mnt/daxadsl/csv")

# COMMAND ----------


df = spark.read \
  .option("header", "true") \
  .option("inferSchema", False) \
  .csv("/mnt/daxadsl/csv")  \
  
display(df)


df_cars = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/daxadsl/csv')
display(df_cars)


# COMMAND ----------

from shutil import copyfile

with open("/dbfs/tmp/test_dbfs.txt", 'w') as f:
  f.write("Apache Spark is awesome!\n")
  f.write("End of example!")
  
  
#copyfile('/home/deng/ddd.txt', '/dbfs/tmp/demo3.txt')


# COMMAND ----------

# this is a test in master branch
# this is a test in master branch2

df = spark.read \
  .option("header", "true") \
  .option("inferSchema", True) \
  .csv("/mnt/daxadsl/csv")  \
  
df.printSchema()
display(df)
