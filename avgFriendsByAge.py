from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("avgFriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])

    return (age, numFriends)

lines = sc.textFile("./data/fakefriends.csv")
rdd = lines.map(parseLine)

totalsByAge = rdd.mapValues(lambda val: (val,1))\
    .reduceByKey(lambda key1Val, key2Val: (key1Val[0] + key2Val[0], key1Val[1] + key2Val[1]))

averagesByAge = totalsByAge.mapValues(lambda val : val[0] // val[1])
sortedAveragesByAge = averagesByAge.sortByKey()
results = sortedAveragesByAge.collect()

for result in results:
    print(result)