from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("customerSpendings")
sc = SparkContext(conf = conf)


def getCustomerAndSpendings(line):
    row = line.split(',')
    custId = int(row[0])
    amount = float(row[2])
    return (custId, amount)


lines = sc.textFile("./data/customer-orders.csv")
parsedLines = lines.map(getCustomerAndSpendings)

totalAmounts = parsedLines.reduceByKey(lambda v1, v2 : v1 + v2)
reversedTotalAmounts = totalAmounts.map(lambda entry: (entry[1],entry[0]))
sortedTotalAmounts = reversedTotalAmounts.sortByKey(False) #false gives descending order

results = sortedTotalAmounts.collect()

print("\nTotal spendings by each customer: \n")
for amount, customer in results:
    print( "customer-"+str(customer) + "\t {:.2f}$".format(amount))