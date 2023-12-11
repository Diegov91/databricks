# Databricks notebook source
df= spark.read.format("csv").option('header',True).option('inferSchema',True).load("dbfs:/mnt/demo/spotify_songs.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import lit
df_addColumn=df.withColumn("Location",lit("Mumbai"))
display(df_addColumn)

# COMMAND ----------

from pyspark.sql.functions import concat
df_addColum = df.withColumn("Bonus",df_addColumn.track_popularity * 1).withColumn("Name",concat("track_name",lit("-"),"track_artist"))
display(df_addColum)

# COMMAND ----------

df_addColum=df_addColum.withColumnRenamed("name","Album-Artist")
display(df_addColum)

# COMMAND ----------

df_addColum=df_addColum.drop("Album-Artist").drop("Bonus")
display(df_addColum)

# COMMAND ----------

this is a test
