from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def main():
    # Create Spark context
    conf = SparkConf().setAppName("Task3").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()

    # Read the data
    nodes = sc.textFile("./nodes.txt")
    names = sc.textFile("./names.txt")

    # a) 
    nodesRDD = nodes.map(lambda line: line.strip().split()) \
                        .map(lambda x: (int(x[0]), float(x[1])))
    nodesDF = spark.createDataFrame(nodesRDD, ["id", "page_rank"])
    print("Task a) DataFrame of node IDs and their PageRank values:")
    nodesDF.show()
    nodesDF.createOrReplaceTempView("page_rank")

    # b)
    print("\n")
    taskB = spark.sql("SELECT page_rank FROM page_rank WHERE id = 2")
    print("Task b) PageRank of node with ID 2:")
    taskB.show()

    # c)
    print("\n")
    taskC = spark.sql("SELECT * FROM page_rank ORDER BY page_rank DESC LIMIT 5")
    print("Task c) Node with the highest PageRank value:")
    taskC.show()

    # d)
    print("\n")
    namesRDD = names.map(lambda line: line.strip().split(",", 1)) \
                    .map(lambda x: (int(x[0]), x[1]))
    namesDF = spark.createDataFrame(namesRDD, ["id", "names"])
    namesDF.show()
    namesDF.createOrReplaceTempView("names")

    # e)
    print("\n")
    taskE = spark.sql("SELECT n.id, n.name, p.page_rank FROM names n JOIN page_rank p ON n.id = p.id")
    print("Task e) Node names with their PageRank values:")
    taskE.show()

    return

main()
