from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.appName("EngagementByAgeGroup").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Join posts and users datasets on UserID
joined_df = posts_df.join(users_df, on="UserID", how="inner")

# Group by AgeGroup and calculate average likes and retweets
engagement_df = joined_df.groupBy("AgeGroup").agg(
    avg(col("Likes")).alias("AvgLikes"),
    avg(col("Retweets")).alias("AvgRetweets")
).orderBy(col("AvgLikes").desc())

# Save result
engagement_df.coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)
