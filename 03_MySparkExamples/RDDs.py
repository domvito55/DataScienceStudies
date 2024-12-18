import findspark
findspark.init()
findspark.find()
from pyspark.sql import SparkSession
from pyspark.sql.types import *

#Create a local Spark session
spark = (
    SparkSession
        .builder
        .appName("RDDApp")
        .master("local[4]")
        .getOrCreate()
)

# Get the Spark Context from Spark Session
sc = spark.sparkContext

# Create a RDD from a list
numbersRdd = sc.parallelize([1,2,3,4,5])

# Get the number of partitions
numbersRdd.getNumPartitions()

# return the rdd as a list
output = numbersRdd.collect()
print(output)

# return the 2 first element of the rdd
numbersRdd.take(2)

# return the first element of the rdd
numbersRdd.first()

# Create a RDD from a list of lists
employeeRdd = sc.parallelize(
    [
        [1, "Matheus", 10000],
        [2, "Vinicius", 20000],
        [3, "Ferreira", 30000],
        [4, "Figueiredo", 40000],
        [5, "Teixeira", 50000],
    ]
)

# Get the first element of the RDD
employeeRdd.first()

# read a file and create a RDD
taxiZoneRdd = sc.textFile("apache-spark-3-fundamentals/DataFiles/Raw/TaxiZones.csv")

# get the 10 first elements of the RDD
taxiZoneRdd.take(10)

# get the number of partitions
taxiZoneRdd.getNumPartitions()

# splits the rows by comma
taxiZoneRddWithColsRdd = (
    taxiZoneRdd
        .map(lambda zone: zone.split(","))
)

# get the 5 first elements of the RDD
taxiZoneRddWithColsRdd.take(5)

# filter the rows that have the word "Manhattan" in the second column
# and the word "Central" in the third column
filteredZoneRdd = (
    taxiZoneRddWithColsRdd
        .filter(lambda zoneRow: zoneRow[1] == "Manhattan"
                   and zoneRow[2].lower().startswith("central")
               )
)

# get the 5 first elements of the RDD
filteredZoneRdd.take(5)

# filter the rows that have the first column as an even number
evenZones = (
    taxiZoneRddWithColsRdd
        .filter(lambda zoneRow: int(zoneRow[0]) % 2 == 0)
)

# get the 10 first elements of the RDD
evenZones.take(10)

# create a pair RDD from the numbers RDD [1,2,3,4,5], the key is the number and
# the value is the square of the number
import math
numWithSquareRootRdd = (
    numbersRdd
        .map(
            lambda num: (
                num,
                math.sqrt(num)
            )
        )
)

# get the 5 first elements of the RDD
numWithSquareRootRdd.take(5)

# create a pair RDD from the taxiZone RDD, the key is the first column and
# the value is the rest of the columns
taxiPair = (
    taxiZoneRddWithColsRdd
        .map(lambda row:(
                row[0],
                row[1:]
            )
        )
)

# get the 5 first elements of the RDD
taxiPair.take(5)

# create a pair RDD from the taxiZone RDD, the key is the second column and
# the value is 1
taxiPair = (
    taxiZoneRddWithColsRdd
        .map(lambda row:(
                row[1],
                1
            )
        )
)

# get the 5 first elements of the RDD
taxiPair.take(5)

# sum the values of the pair RDD giving us the count of each borough
boroughCountRdd = (
    taxiPair
        .reduceByKey(
            lambda value1, value2: value1 + value2
        )
)

boroughCountRdd.take(5)

# sort the pair RDD by the key (borough name)
boroughCountRdd.sortByKey().take(5)

# show the keys of the pair RDD
print(boroughCountRdd.keys().collect())

# show the values of the pair RDD
boroughCountRdd.values().collect()

#find the distinct values of the pair RDD
print(taxiPair.distinct().collect())
