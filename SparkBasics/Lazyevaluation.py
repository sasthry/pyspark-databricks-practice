# Databricks notebook source
spark


# COMMAND ----------

data=[(1,"lisa","lisa@gmail.com"),
      (2,"john","john@gmail.com"),
      (3,"mary","mary@gmail.com"),
      (4,"peter","peter@gmail.com"),
      (5,"jack","jack@gmail.com"),
      (6,"rose","rose@gmail.com"),
      (7,"jane","jane@gmail.com"),
      (8,"jim","jim@gmail.com"),
      (9,"tom","tom@gmail.com"),
      (10,"jerry","jerry@gmail.com")]
column=["id","name","gmail"]
df=spark.createDataFrame(data,column)
df_new=df.select("id","name")
df_filter=df_new.filter(df_new.id%2)


# COMMAND ----------

from pyspark.sql.functions import * 



# COMMAND ----------

df=spark.read.format("csv")\
             .option("header","True")\
             .load("/Volumes/workspace/default/source/orders/orders.csv")

# COMMAND ----------

df=df.repartition(2)
df=df.select("order_id","customer_id")
df=df.filter(col("order_id")==1001)
df=df.groupBy("order_id").count()

# COMMAND ----------

df.display()


# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import * 
df_orders=spark.read.format("csv")\
             .option("header","True")\
             .load("/Volumes/workspace/default/source/orders/orders.csv")
             

# COMMAND ----------

display(df_orders.withColumn("partitionid",spark_partition_id())\
                      .groupBy("partitionid")\
                      .agg(count("*")).alias("partitioncount")\
                      .orderBy("partitionid"))

df=df_orders.repartition(2)

# COMMAND ----------

display(df.withColumn("partitionid",spark_partition_id())\
                      .groupBy("partitionid")\
                      .agg(count("*")).alias("partitioncount")\
                      .orderBy("partitionid"))

# COMMAND ----------

df_orders=df.coalesce(1)

# COMMAND ----------

display(df_orders.withColumn("partitionid",spark_partition_id())\
                      .groupBy("partitionid")\
                      .agg(count("*")).alias("partitioncount")\
                      .orderBy("partitionid"))