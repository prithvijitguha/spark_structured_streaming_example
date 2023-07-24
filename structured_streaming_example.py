# Databricks notebook source
from pyspark.sql import SparkSession 

# You don't need to define the SparkSession entry point if you're on Databricks,
# however its good to keep your code compatible
# with multiple Spark ecosystems
spark = SparkSession.builder.getOrCreate() 

print(spark.version)

# COMMAND ----------

# MAGIC %sh 
# MAGIC
# MAGIC python --version

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
    .load("dbfs:/FileStore/source_directory/*.csv", schema=sample_dataset_schema, header=True)

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/source_directory/") # make target directory for uploading data


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



# Now we an write the stream to 
# a target location and specify a checkpoint as well
df_standardized.writeStream\
  .format("parquet")\
  .outputMode("append")\
  .trigger(availableNow=True)\
  .partitionBy("snapshot_date")\
  .option("checkpointLocation", "dbfs:/sample_dataset_checkpoint/")\
  .toTable("hrz_sample_dataset")\



# COMMAND ----------

display(spark.read.table("hrz_sample_dataset"))

# COMMAND ----------

# Incase you want to use a continuous stream, which is perpetually on
# Default trigger (runs micro-batch as soon as it can)
df_standardized.writeStream\
  .format("console") \
  .start()

# ProcessingTime trigger with two-seconds micro-batch interval
df_standardized.writeStream \
  .format("console") \
  .trigger(processingTime='2 seconds') \
  .start()

# COMMAND ----------

# Alternate forms of reading data, read from table
# In this case we only update the pipeline the table is updated 
df_sink_table_format = spark.readStream.table("hrz_sample_dataset")

# COMMAND ----------

# Import functions
from pyspark.sql.functions import col, current_timestamp

# Define variables used in code below
file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))

# Databricks Page: https://docs.databricks.com/getting-started/etl-quick-start.html

# COMMAND ----------

# MAGIC %pip install flowrunner

# COMMAND ----------

from flowrunner import BaseFlow, end, start, step
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import input_file_name, col, current_timestamp


spark = SparkSession.builder.getOrCreate()


class IngestionFlow(BaseFlow):
    @start
    @step(next=["standardize_data"])
    def read_data_stream(self):
        """
        This method is used to read the datastream
        """
        # The schema specification is only required for json and csv. Parquet, Delta and orc should be fine without it
        sample_dataset_schema = StructType([\
            StructField("Firstname",StringType(),True),\
            StructField("Lastname",StringType(),True),\
            StructField("Jedi_Rank", StringType(), True),\
            StructField("IsCouncilMember", BooleanType(), True),\
            StructField("snapshot_date", StringType(), True) 
        ])

        # Read the dataframe into a stream specifying the schema and load data directory
        self.df_sink = spark.readStream.format("csv")\
            .load("dbfs:/FileStore/source_directory/*.csv", schema=sample_dataset_schema, header=True)

        

    @step(next=["write_datastream"])
    def standardize_data(self):
        """
        This method we standardize the data a little bit with some column renaming 
        """        
        # we can add some other parameters like renaming the columns to a more standardized snake_case format 
        # and adding input file name and record load timestamp 
        self.df_standardized = self.df_sink.select(
            col("Firstname").alias("first_name"),
            col("Lastname").alias("last_name"),
            col("Jedi_Rank").alias("jedi_rank"),
            col("IsCouncilMember").alias("is_council_member"),
            col("snapshot_date").cast("date"),
            input_file_name().alias("source_file_name"), 
            current_timestamp().alias("record_load_timestamp"), 
        )

    @end
    @step
    def write_datastream(self):
        """
        Here we write the dataframe to final DBFS/HDFS location
        """
        # Now we an write the stream to 
        # a target location and specify a checkpoint as well
        self.df_standardized.writeStream\
        .format("parquet")\
        .outputMode("append")\
        .trigger(availableNow=True)\
        .partitionBy("snapshot_date")\
        .option("checkpointLocation", "dbfs:/sample_flow_checkpoint/")\
        .toTable("hrz_sample_dataset_flow")



ingestion_flow = IngestionFlow() # create instance of pipeline 
ingestion_flow.display() # display your flow without running it

# COMMAND ----------

ingestion_flow.run()

# COMMAND ----------

# Example of Abstract Factory Ingestion Pipeline
from dataclasses import dataclass


@dataclass
class IngestionPipeline:
    """Sample Ingeestion Pipeline

    This class is used as a reusable abstract factory to build new pipelines

    Examples:

    .. code-block:: python

        ingestion_pipeline = IngestionPipeline()

        ingestion_pipeline.read_data_stream(
            file_path="dbfs:/FileStore/source_directory/*.csv",
            file_type="csv",
            schema=sample_dataset_schema,
            header=True
        )

        ingestion_pipeline.write_data_stream(
            data_format = "delta",
            table_name = "some_target_table"
        )
    """

    def __post_init__(self):
        """Method invoked post creating instance to get a SparkSession"""
        self.spark = SparkSession.builder.getOrCreate()

    def read_data_stream(
        self, 
        file_path: str, 
        file_type: str, 
        **kwargs
    ):
        """Method to read data into a stream using Spark Structured Streaming

        Kwargs is used so we can pass any others arguments that may be required. 
        Since this method can be overriden, users
        can override this to add any other stream capabilities that may not be covered

        Args:
            - file_path(str): A path or directory to look for, this argument accepts glob patterns.
             eg. "dbfs:/some_directory_name/*.csv".
            - file_type(str): A type of data format to read or look 
            for eg. "json", "parquet", "delta", 
            "csv"
            - kwargs(dict): Any additional arguments to be added. eg. header, schema, etc
        """
        self.stream_query = spark.readStream.format(file_type).load(file_path, **kwargs)

    def write_data_stream(
        self,
        data_format: str,
        table_name: str,
        output_mode: str = "append",
        checkpoint_location: str = None,
    ):
        """Method to write structured streaming dataframe to dbfs/hdfs

        We create a reusable method to avoid writing boilerplate code again and again. 
        This method does not require a checkpoint to be
        mentioned, but it instead will create a checkpoint for you.
        This method can be overriden, users can override this to add 
        any writing functionality that might not be covered.

        Args:
            - data_format(str): Final write format to write in 
            eg. console, parquet, delta
            - table_name(str): A name for SparkSQL/Hive table to save as
            - output_mode(str, optional): Output mode, options are append, 
            update or complete(meaning overwrite)
            - checkpoint_location(str, optional): An optional argument for 
            checkpoint location. If not specified, then this
            method will use a default location for checkpoint location.

        Note:
            Although 'checkpoint_location' is optional, if not specified a default location will be 
            used, checkpoint is required
            for tracking progress
        """
        if not checkpoint_location:
            checkpoint_location = f"dbfs:/_checkpoints/{table_name}"
        self.stream_query.writeStream.format(data_format).outputMode(
            output_mode
        ).trigger(availableNow=True).option(
            "checkpointLocation", checkpoint_location
        ).toTable(
            table_name
        )


# COMMAND ----------

# Handling history data so that it does break your pipeline 
# Read the dataframe into a stream specifying the schema and load data directory
# We also mention the max files per trigger so that we only 20 per trigger pull
df_sink = spark.readStream.format("csv")\
    .option("maxFilesPerTrigger", 20)\
    .load("dbfs:/FileStore/source_directory/*.csv", schema=sample_dataset_schema, header=True)

# COMMAND ----------


