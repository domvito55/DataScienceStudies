import findspark

findspark.init()
findspark.find()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# starting a local spark session
spark = (SparkSession
            .builder
            .appName("DafaFrameAPI")
            .master("local[4]")

            .config("spark.dynamicaAllocation.enable","false")
            .config("spark.sql.adaptative.enable","false")

            # enable Hive support (persistent storage)
            # if this line is removed, the catalog will be in-memory
            .enableHiveSupport()

            .getOrCreate()
        )

# get the spark context from the spark session
sc = spark.sparkContext

# defining schema for yellow taxi
yellowTaxiSchema = (StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOlocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True)
]))

# create a dataframe from a csv file with a defined schema
yellowTaxiDF = (
    spark
        .read
        .option("header", "true")
        .schema(yellowTaxiSchema)
        .csv("apache-spark-3-fundamentals/DataFiles/Raw/YellowTaxis_202210.csv")
)
yellowTaxiDF.printSchema()

# show the databases in Hive metastore (the catalog)
spark.sql("SHOW DATABASES").show()

# create a database in Hive metastore
spark.sql("CREATE DATABASE IF NOT EXISTS TaxiDB")

# show the databases in Hive metastore (the catalog)
spark.sql("SHOW DATABASES").show()

# ------------- create a managed table in the TaxiDB database
yellowTaxiDF.write.mode("overwrite").saveAsTable("TaxiDB.YellowTaxis")

# show the tables in the TaxiDB database
spark.sql("SHOW TABLES IN TaxiDB").show()

# read table using spark.sql
outputDF = spark.sql(
  """
    SELECT *
      FROM TaxiDB.YellowTaxis
    LIMIT 5
  """
).show()

# read table using python
outputDF = (
  spark
    .read
    .table("TaxiDB.YellowTaxis")
)

outputDF.show(5)

# check table details
spark.sql(
  """
    DESCRIBE TABLE EXTENDED TaxiDB.YellowTaxis
  """
).show(50, truncate=False)

# ----------------- create an external table in the TaxiDB database
(
  yellowTaxiDF
    .write
    .mode("overwrite")
    # this option is used to create an external table
    .option("path", "apache-spark-3-fundamentals/DataFiles/Output/YellowTaxis")
    #.option("format", "csv") # default format is parquet
    .saveAsTable("TaxiDB.YellowTaxis")
)

# check table details
spark.sql(
  """
    DESCRIBE TABLE EXTENDED TaxiDB.YellowTaxis
  """
).show(50, truncate=False)

# Dropping the table
# as this is not a managed table, the data will not be deleted only the metadata
spark.sql("DROP TABLE IF EXISTS TaxiDB.YellowTaxis")

# Creating a table (metadata only) from files
spark.sql(
  """
    CREATE TABLE IF NOT EXISTS TaxiDB.YellowTaxis

    USING PARQUET

    LOCATION 'apache-spark-3-fundamentals/DataFiles/Output/YellowTaxis.parquet'
  """
)



