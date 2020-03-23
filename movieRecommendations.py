import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, Rating

def loadMovieNames():
    movieNames = {}
    with open("./data/ml-100k/u.ITEM", encoding='ascii', errors="ignore") as file:
        for line in file:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


conf = SparkConf().setMaster("local[*]").setAppName("MovieRecommendationsUsingALS")
sc = SparkContext(conf = conf)
#sc.setCheckpointDir('checkpoint')

print("\nLoading movie names...")
nameDict = loadMovieNames()

data = sc.textFile("./data/ml-100k/u.data")

ratings = data.map(lambda line: line.split()).map(lambda line: Rating(int(line[0]), int(line[1]), float(line[2]))).cache()

# train the Alternating Least Squares recommendation model
print("\nTraining recommendation model...")
rank = 10
numIterations = 6

model = ALS.train(ratings, rank, numIterations)

# the user for whom we want to get the recommendations
userID = int(sys.argv[1])

print("\nRatings for user ID " + str(userID) + ":")
userRatings = ratings.filter(lambda line: line[0] == userID)
for rating in userRatings.collect():
    print (nameDict[int(rating[1])] + ": " + str(rating[2]))

print("\nTop 15 recommendations for user ID {} are:".format(userID))
recommendations = model.recommendProducts(userID, 15)
for recommendation in recommendations:
    print (nameDict[int(recommendation[1])] + " score " + str(recommendation[2]))