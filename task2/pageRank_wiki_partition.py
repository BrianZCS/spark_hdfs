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
    .appName("Page Rank Wiki Partition (300, 300)")
    .config("spark.some.config.option", "some-value")
    .getOrCreate())

    files_name = "hdfs://10.10.1.1:9000/data/enwiki"
    lines_0 = spark.read.text(files_name).rdd.map(lambda r: r[0])
    pages = lines_0.map(parse_line).toDF(["page", "neighbor"])

    # Initialize ranks for each page
    ranks = pages.select("page").withColumn("rank", F.lit(1.0))
    ranks = ranks.withColumnRenamed("page", "node")
    ranks = ranks.dropDuplicates(["node"])

    ## if we need to partition the data
    pages = pages.repartition(300)
    ranks = ranks.repartition(300)

    # Perform iterations
    iterations = 3
    for _ in range(iterations):
        ranks = pagerank_iter(pages, ranks)

    final_ranks = pages.join(ranks, pages.page == ranks.node, "left").fillna(0.15).select('page', 'rank').dropDuplicates(["page"])
    # save the final ranks
    final_ranks.write.option("header", "true").csv("hdfs://10.10.1.1:9000/data/output_wiki_partition", mode = "overwrite")

    spark.stop()

if __name__ == "__main__":
    main()