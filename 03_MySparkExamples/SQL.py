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
            .getOrCreate()
            # .master("spark://localhost:7077")
            # .config("spark.dynamicaAllocation.enable","false")
            # .config("spark.executor.memory","2g")
            # .config("spark.executor.cores","2")
            # .getOrCreate()
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

# create a temporary view to query the dataframe
yellowTaxiDF.createOrReplaceTempView("YellowTaxis")

# query the dataframe
outputDF = spark.sql(
        "SELECT * FROM YellowTaxis WHERE PULocationID = 171"
    )
outputDF.show(5)

# create a dataframe from a csv file with a different delimiter
greenTaxiDF = (
    spark
        .read
        .option("header", "true")
        .option("delimiter", "\t")
        .csv("apache-spark-3-fundamentals/DataFiles/Raw/GreenTaxis_*.csv")
)

# create a temporary view to query the dataframe
greenTaxiDF.createOrReplaceTempView("GreenTaxis")

# query the dataframe
outputDF = spark.sql(
  """
  SELECT 'Yellow' AS TaxiType,
    lpep_pickup_datetime AS PickupTime, 
    lpep_dropoff_datetime AS DropOffTime, 
    PULocationID AS PickupLocationID, 
    DOlocationID AS DropOffLocationID
  FROM YellowTaxis

  UNION ALL

  SELECT 'Green' AS TaxiType,
    lpep_pickup_datetime AS PickupTime, 
    lpep_dropoff_datetime AS DropOffTime, 
    PULocationID AS PickupLocationID, 
    DOLocationID AS DropOffLocationID
  FROM GreenTaxis 
  """
).show(5)

# define schema for taxi zones
taxiZoneSchema = "LocationID INT, Borough STRING, Zone STRING, ServiceZone STRING"

# create a dataframe from a csv file with a defined schema
taxiZoneDF = (
    spark
        .read
        .schema(taxiZoneSchema)
        .csv("apache-spark-3-fundamentals/DataFiles/Raw/TaxiZones.csv")
)

# create a temporary view to query the dataframe
taxiZoneDF.createOrReplaceGlobalTempView("TaxiZones")
taxiZoneDF.show(5)

# Create a report out of number of rides grouped by borough and type of taxi
outputDF = spark.sql(
    """
    SELECT 
        Borough, 
        TaxiType, 
        COUNT(*) AS TotalTrips
    FROM global_temp.TaxiZones LEFT JOIN (
      SELECT 'Yellow' AS TaxiType, PUlocationID
        FROM YellowTaxis
      
      UNION ALL

      SELECT 'Green' AS TaxiType, PULocationID
        FROM GreenTaxis
    ) AS AllTaxis

    ON  ALLTaxis.PULocationID = TaxiZones.LocationID

    GROUP BY Borough, TaxiType

    ORDER BY Borough, TaxiType
    """
).show(5)