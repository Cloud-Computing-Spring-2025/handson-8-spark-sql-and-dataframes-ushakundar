from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col

spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Categorize sentiment
posts_df = posts_df.withColumn(
    "Sentiment",
    when(col("SentimentScore") > 0.3, "Positive")
    .when((col("SentimentScore") >= -0.3) & (col("SentimentScore") <= 0.3), "Neutral")
    .otherwise("Negative")
)

# Group by sentiment and calculate average likes and retweets
sentiment_stats = posts_df.groupBy("Sentiment").agg(
    avg(col("Likes")).alias("AvgLikes"),
    avg(col("Retweets")).alias("AvgRetweets")
).orderBy(col("AvgLikes").desc())

# Save result
sentiment_stats.coalesce(1).write.mode("overwrite").csv("outputs/sentiment_engagement.csv", header=True)
