from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions


def loadMovieNames():
    movieNames = {}
    with open("./data/ml-100k/u.ITEM") as file:
        for line in file:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/tmp").appName("PopularMovies").getOrCreate()

nameDict = loadMovieNames()

lines = spark.sparkContext.textFile("./data/ml-100k/u.data")

# convert it to a RDD of Row objects and then to a DataFrame
movies = lines.map(lambda x: Row(movieID =int(x.split()[1])))
movieDataFrame = spark.createDataFrame(movies)

# get the movies with highest number of ratings
topMovieIDs = movieDataFrame.groupBy("movieID").count().orderBy("count", ascending=False).cache()
topMovieIDs.show()

top10 = topMovieIDs.take(10)

for result in top10:
    print("%s: %d" % (nameDict[result[0]], result[1]))

spark.stop()