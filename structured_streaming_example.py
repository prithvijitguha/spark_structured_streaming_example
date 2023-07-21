# Databricks notebook source
from pyspark.sql import SparkSession

# You don't need to define the SparkSession entry point if you're on Databricks,
# however its good to keep your code compatible
# with multiple Spark ecosystems
spark = SparkSession.builder.getOrCreate()

print(spark.version)

!python --version

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, BooleanType


# The schema specification is only required for json and csv. Parquet, Delta and orc should be fine without it
sample_dataset_schema = StructType([\
    StructField("Firstname",StringType(),True),\
    StructField("Lastname",StringType(),True),\
    StructField("Jedi_Rank", StringType(), True),\
    StructField("IsCouncilMember", BooleanType(), True),\
    StructField("snapshot_date", StringType(), True)
])

# COMMAND ----------

# Read the dataframe into a stream specifying the schema and load data directory
df_sink = spark.readStream.format("csv")\
    .schema(sample_dataset_schema)\
    .option("header", True)\
    .load("dbfs:/FileStore/source_directory/")

# COMMAND ----------

from pyspark.sql.functions import input_file_name, col, current_timestamp

# we can add some other parameters like renaming the columns to a more standardized snake_case format
# and adding input file name and record load timestamp
df_standardized = df_sink.select(
    col("Firstname").alias("first_name"),
    col("Lastname").alias("last_name"),
    col("Jedi_Rank").alias("jedi_rank"),
    col("IsCouncilMember").alias("is_council_member"),
    col("snapshot_date").cast("date"),
    input_file_name().alias("source_file_name"),
    current_timestamp().alias("record_load_timestamp"),
)

# COMMAND ----------

# let's create our target database for reference
spark.sql("CREATE DATABASE IF NOT EXISTS harmonized_data")

# Now we an write the stream to
# a target location and specify a checkpoint as well
df_standardized.writeStream\
  .format("parquet")\
  .outputMode("append")\
  .trigger(availableNow=True)\
  .partitionBy("snapshot_date")\
  .option("checkpointLocation", "dbfs:/sample_dataset_checkpoint/")\
  .toTable("harmonized_data.hrz_sample_dataset")\



# COMMAND ----------

spark.read.table("harmonized_data.hrz_sample_dataset").show() # or display() if you're using this inside databricks

# COMMAND ----------


