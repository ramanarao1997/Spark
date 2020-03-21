import sys
from pyspark import SparkConf, SparkContext
from math import sqrt


def loadMovieNames():
    movieNames = {}
    with open("./data/ml-100k/u.ITEM", encoding='ascii', errors='ignore') as file:
        for line in file:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def makePairs( userRatings ):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))


def filterDuplicates( userRatings ):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2


def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0

    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0

    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)


conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf = conf)

print("\nLoading movie names")
nameDict = loadMovieNames()

data = sc.textFile("./data/ml-100k/u.data")

# map ratings to key/value pairs (user ID => movie ID, rating)
ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

# self-join to find every pair of movies rated by a user.
joinedRatings = ratings.join(ratings)

# at this point RDD consists of userID => ((movieID, rating), (movieID, rating))
# filtering duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

# map to get (movie1, movie2) as key and (rating1, rating2) as value.
moviePairs = uniqueJoinedRatings.map(makePairs)

# group by movie pairs to compute similarity
moviePairRatings = moviePairs.groupByKey()

# we now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2)
# compute similarity and cache it
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

# uncomment below lines to save the results if desired
#moviePairSimilarities.sortByKey()
#moviePairSimilarities.saveAsTextFile("movie-similarities-result")

# take the movie that we care about as command-line argument
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 50

    movieID = int(sys.argv[1])

    # filter for similar movies based on our thresholds
    filteredResults = moviePairSimilarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
        and pairSim[1][0] > scoreThreshold \
        and pairSim[1][1] > coOccurenceThreshold)

    # sort by quality score.
    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + nameDict[movieID])
    for result in results:
        (sim, pair) = result
        # display the similar movie that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))