from pyspark import SparkConf, SparkContext
import re


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("./data/book.txt")

# using flatMap() gives a new RDD with each word as an element
# if a map() were used instead, new RDD would only have elements as lines of words (as the original RDD)
words = input.flatMap(normalizeWords)
wordCounts = words.map(lambda w: (w, 1)).reduceByKey(lambda w1, w2:  w1 + w2)
wordCountsSorted = wordCounts.map(lambda entry: (entry[1], entry[0])).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    count = result[0]
    word = result[1]

    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + ":\t" + str(count))
