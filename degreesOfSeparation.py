from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

# IDs of the characters we wish to find the degrees of separation between
startCharacterID = 859 # Captain America
targetCharacterID = 5501 # Stryker

hitCounter = sc.accumulator(0)


def convertToNodes(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    if (heroID == startCharacterID):
        color = 'GRAY'
        distance = 0

    return (heroID, (connections, distance, color))


def createStartingGraphRdd():
    inputFile = sc.textFile("./data/Marvel graph.txt")
    return inputFile.map(convertToNodes)


def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    # nodes having color as GRAY need to be explored further
    if (color == 'GRAY'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'

            # if the target node is as a GRAY node, increment the accumulator
            if (targetCharacterID == connection):
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        # color the node black once it is explored
        color = 'BLACK'

    # append the input node to results too
    results.append( (characterID, (connections, distance, color)) )
    return results


def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # preserve all unexplored nodes
    if (len(edges1) > 0):
        edges.extend(edges1)

    if (len(edges2) > 0):
        edges.extend(edges2)

    # preserve minimum distance
    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)


# Main program
iterationRdd = createStartingGraphRdd()

for iteration in range(1, 11):
    print("Running BFS iteration #{}".format(iteration))

    # create new vertices during BFS
    mapped = iterationRdd.flatMap(bfsMap)

    # here count() action actually triggers things as Spark does lazy evaluation
    print("Processing " + str(mapped.count()) + " values.")

    # if accumulator > 0, we are done!
    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
            + " different direction(s).")
        break

    # combine data for each character ID
    iterationRdd = mapped.reduceByKey(bfsReduce)