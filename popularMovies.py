from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)


def getMovieNames():
    movieNames = {}
    with open("./data/ml-100k/u.item") as file:
        for line in file:
            row = line.split('|')
            movieNames[int(row[0])] = row[1]
    return movieNames


moviesBroadcast = sc.broadcast(getMovieNames())
lines = sc.textFile("./data/ml-100k/u.data")

movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map(lambda entry: (entry[1], entry[0]))
sortedMovieCounts = flipped.sortByKey()

namedMovieCounts = sortedMovieCounts.map(lambda entry: (moviesBroadcast.value[entry[1]], entry[0]))
results = namedMovieCounts.collect()

for result in results:
    print(result[0] + "\t {} Reviews".format(result[1]))
