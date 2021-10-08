# Databricks notebook source
# MAGIC %fs ls /test

# COMMAND ----------


df = spark.read \
  .option("header", "true") \
  .option("inferSchema", True) \
  .csv("/mnt/daxadsl/csv")  \
  
display(df)


df_cars = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/daxadsl/csv')
display(df_cars)
