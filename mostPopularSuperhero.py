from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)


def countCoOccurences(line):
    heroIDs = line.split()
    return (int(heroIDs[0]), len(heroIDs) - 1)


def getHeroNames(line):
    row = line.split('\"')
    return (int(row[0]), row[1].encode("utf8"))


heroNames = sc.textFile("./data/Marvel names.txt")
namesRDD = heroNames.map(getHeroNames)

connections = sc.textFile("./data/Marvel graph.txt")
connectionsRDD = connections.map(countCoOccurences)

totalCoOccurrences = connectionsRDD.reduceByKey(lambda x, y: x + y)
flipped = totalCoOccurrences.map(lambda entry: (entry[1], entry[0]))

mostPopularHero = flipped.max()
mostPopularHeroName = namesRDD.lookup(mostPopularHero[1])[0]

print("\n{0} is the most popular superhero in Marvel Universe with {1} co-appearences.\n".\
      format(mostPopularHeroName.decode(),mostPopularHero[0]))