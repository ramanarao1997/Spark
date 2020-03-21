from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections

# create a SparkSession (Note: the config section is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/tmp").appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("./data/fakefriends.csv")
people = lines.map(mapper)

# Infer the schema and register the DataFrame as a table to run SQL queries.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# the results of SQL queries are RDDs
for teen in teenagers.collect():
  print(teen)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()