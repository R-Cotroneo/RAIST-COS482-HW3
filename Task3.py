import csv
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def main():
    # Create Spark context
    conf = SparkConf().setAppName("Task3").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()

    # Read the data
    nodes = sc.textFile("./pagerank_output.txt")
    names = sc.textFile("./names.txt")

    # a) 
    nodesRDD = nodes.map(lambda line: line.strip().split()) \
                    .map(lambda x: (x[0], x[1]))
    nodesDF = spark.createDataFrame(nodesRDD, ["id", "page_rank"])
    nodesDF.createOrReplaceTempView("page_ranks")
    print("Task a) DataFrame of node IDs and their PageRank values:")
    nodesDF.show()

    # b)
    print("\n")
    taskB = spark.sql("SELECT page_rank FROM page_ranks WHERE id = 2")
    print("Task b) PageRank of node with ID 2:")
    taskB.show()

    # c)
    print("\n")
    taskC = spark.sql("SELECT * FROM page_ranks ORDER BY page_rank DESC LIMIT 1")
    print("Task c) Node with the highest PageRank value:")
    taskC.show()

    # d)
    print("\n")
    namesRDD = names.map(lambda line: line.strip().split()) \
                    .map(lambda x: (x[0], x[1]))
    namesDF = spark.createDataFrame(namesRDD, ["id", "name"])
    namesDF.createOrReplaceTempView("names")
    print("Task d) Dataframe with node IDs and name associated with them:")
    namesDF.show()

    # e)
    print("\n")
    taskE = spark.sql("SELECT n.id, n.name, p.page_rank FROM names n JOIN page_ranks p ON n.id = p.id")
    print("Task e) Node names with their PageRank values:")
    taskE.show()
    with open("task3_e.csv", "w") as file:
        fieldnames = ["id", "name", "page_rank"]
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=",")
        writer.writeheader()
        for row in taskE.collect():
            writer.writerow({
                "id": row["id"],
                "name": row["name"],
                "page_rank": row["page_rank"]
            })
    
    return

main()
