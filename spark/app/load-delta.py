import sys
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from delta import *

##########################
# You can configure master here if you do not pass the spark.master paramenter in conf
##########################
#master = "spark://spark:7077"
#conf = SparkConf().setAppName("Spark Hello World").setMaster(master)
#sc = SparkContext(conf=conf)
#spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Create spark context
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
.config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")#\

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Get the second argument passed to spark-submit (the first is the python app)
logFile = sys.argv[1]

print(logFile)

# Read file
df_movies_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .load(f"{logFile}")
)
    
df_movies_csv.write.format("delta").mode("append").save("/usr/local/spark/resources/data/raw/movies")

# Print result
print(f"Lines with {df_movies_csv.count()}")
