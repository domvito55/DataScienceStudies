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

            .getOrCreate()
        )

# get the spark context from the spark session
sc = spark.sparkContext

cabsDF = (
  spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("apache-spark-3-fundamentals/DataFiles/Raw/Cabs.csv")
)

cabsDF.createTempView("Cabs")

cabsDF.show(10, truncate=False)

# ----------------- User Defined Functions (UDFs) -----------------
def convertCase(strValue: str):
  result = ""

  nameArray = strValue.split(",")

  for name in nameArray:
    result = (
      result +
      name[0:1].upper() +
      name[1:].lower() +
      ", "
    )
  result = result[:-2]
  return result

# register the UDF to be used with python
converCaseUDF = udf(convertCase, StringType())

(
  cabsDF
    .select(
      "Name",
      converCaseUDF("Name").alias("FormattedName")
    )  
).show(10, truncate=False)

# register the UDF to be used with SQL
spark.udf.register("convertCaseUDF", convertCase, StringType())

spark.sql(
  """
  SELECT
    Name,
    convertCaseUDF(Name) AS FormattedName
  FROM Cabs
  """
).show(10, truncate=False)  
