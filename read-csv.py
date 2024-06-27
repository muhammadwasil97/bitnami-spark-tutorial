from pyspark.sql import SparkSession
import time
spark = SparkSession.builder.appName('CSV read spark').getOrCreate()
    
df = spark.read.option("header", True).csv("zipcodes.csv")
# df.show()

query = df.select('ZipCode','City','State','Lat','Long','Country') \
    .where("State IN ('AZ', 'PR', 'FL', 'TX')") \
    .orderBy('State')

query.show()

ts = time.time()

query.write.options(header = 'True', delimiter = ',').csv(f'updated-zipcodes/{ts}')