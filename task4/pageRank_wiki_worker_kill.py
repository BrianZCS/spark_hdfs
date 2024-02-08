from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum
import glob
from pyspark.storagelevel import StorageLevel

def parse_line(line):
    # Split the line into page and neighbor
    parts = line.split("\t")
    return parts[0], parts[1]

def pagerank_iter(pages, ranks, damping_factor=0.85):
    new_pages =  pages.join(ranks, pages.page == ranks.node, "left").fillna(0.15)
    contributions = new_pages.groupBy("page", "rank").count().withColumn("contribution", col("rank")/col("count"))
    contributions_with_neighbor = contributions.join(pages, pages.page == contributions.page, "left")
    # Update ranks for the next iteration
    new_ranks = contributions_with_neighbor.groupBy("neighbor").agg(F.sum("contribution").alias("rank"))
    new_ranks = new_ranks.withColumn("new-rank",0.15 + 0.85 * col("rank"))
    new_ranks = new_ranks.drop("rank")
    new_ranks = new_ranks.withColumnRenamed("neighbor", "node")
    new_ranks = new_ranks.withColumnRenamed("new-rank", "rank")
    return new_ranks

def main():
    spark = (SparkSession
	.builder
	.master("spark://10.10.1.1:7077")#spark://10.10.1.1:7077
    .appName("Page Rank Wiki (Kill Worker)")
    .config("spark.some.config.option", "some-value")
    .getOrCreate())

    # sc = spark.sparkContext
    # Read input file
    #input_file = "hdfs://10.10.1.1:9000/data/web-BerkStan.txt"

    # URI           = sc._gateway.jvm.java.net.URI
    # Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
    # FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    # Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration


    # fs = FileSystem.get(URI("hdfs://10.10.1.1:9000"), Configuration())
    # files_name = []
    # status = fs.listStatus(Path('/data/enwiki'))
    # for fileStatus in status:
    #     files_name.append(fileStatus.getPath().toString())
    files_name = "hdfs://10.10.1.1:9000/data/enwiki"
    lines_0 = spark.read.text(files_name).rdd.map(lambda r: r[0])
    pages = lines_0.map(parse_line).toDF(["page", "neighbor"])

    # for f in files_name:
    #     if f != files_name[0]:
    #         lines = spark.read.text(f).rdd.map(lambda r: r[0])
    #         one_file = lines.map(parse_line).toDF(["page", "neighbor"])
    #         pages = pages.union(one_file)

    # Initialize ranks for each page
    ranks = pages.select("page").withColumn("rank", F.lit(1.0))
    ranks = ranks.withColumnRenamed("page", "node")
    ranks = ranks.dropDuplicates(["node"])

    ## if we need to partition the data
    # pages = pages.repartition("page")
    # ranks  = ranks.repartition("node")
    ## if we want to keep it in memory
    # ranks = ranks.persist(StorageLevel.MEMORY_ONLY)

    # Perform iterations
    iterations = 3
    for _ in range(iterations):
        ranks = pagerank_iter(pages, ranks)


    final_ranks = pages.join(ranks, pages.page == ranks.node, "left").fillna(0.15).select('page', 'rank').dropDuplicates(["page"])
    # save the final ranks
    final_ranks.write.option("header", "true").csv("hdfs://10.10.1.1:9000/data/output_wiki_kill", mode = "overwrite")
    spark.stop()

if __name__ == "__main__":
    main()