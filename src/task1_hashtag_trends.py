from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Split the Hashtags column into individual hashtags
hashtags_df = posts_df.select(explode(split(col("Hashtags"), ",")).alias("Hashtag"))

# Count the frequency of each hashtag
hashtag_counts = hashtags_df.groupBy("Hashtag").count().orderBy(col("count").desc())

# Get the top 10 hashtags
top_hashtags = hashtag_counts.limit(10)

# Save result
top_hashtags.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)
