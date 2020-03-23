from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":

    spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/tmp").appName("LinearRegression").getOrCreate()

    inputLines = spark.sparkContext.textFile("./data/heights_weights.txt")
    data = inputLines.map(lambda x: x.split(",")).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))

    # convert this RDD to a DataFrame
    colNames = ["label", "features"]
    df = data.toDF(colNames)

    trainTest = df.randomSplit([0.8, 0.2])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # training a linear regression model
    linReg = LinearRegression(maxIter = 10, regParam = 0.3, elasticNetParam = 0.8)
    model = linReg.fit(trainingDF)

    # testing the model
    fullPredictions = model.transform(testDF).cache()

    # get the predicted and actual labels.
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("label").rdd.map(lambda x: x[0])

    # zip them together and print
    predictionAndLabel = predictions.zip(labels).collect()

    for prediction in predictionAndLabel:
      print(prediction)


    spark.stop()