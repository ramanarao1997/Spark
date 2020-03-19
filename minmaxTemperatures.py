from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinMaxTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
	
	# temperature in the file is in tens of degrees in Celsius
	# convert to Fahrenheit (after multiplying 0.1 to get actual Celsius reading)
	
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("./data/1800.csv")
parsedLines = lines.map(parseLine)

# filter for only the rows with TMIN or TMAX
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])

# remove entryType as it is no longer needed after filtering
minStationTemps = minTemps.map(lambda x: (x[0], x[2]))
maxStationTemps = maxTemps.map(lambda x: (x[0], x[2]))

# pick min and max for each key (i.e stationID)
minTemps = minStationTemps.reduceByKey(lambda x, y: min(x,y))
maxTemps = maxStationTemps.reduceByKey(lambda x, y: max(x,y))

minResults = minTemps.collect()
maxResults = maxTemps.collect()

print("min temperatures for each station in year 1800 are:")
for result in minResults:
    print(result[0] + "\t{:.2f}F".format(result[1]))
	
	
print("max temperatures for each station in year 1800 are:")
for result in maxResults:
    print(result[0] + "\t{:.2f}F".format(result[1]))