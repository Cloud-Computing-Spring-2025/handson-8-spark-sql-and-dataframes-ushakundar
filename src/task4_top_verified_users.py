from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Filter verified users
verified_users_df = users_df.filter(col("Verified") == True)

# Join posts with verified users
verified_posts_df = posts_df.join(verified_users_df, on="UserID", how="inner")

# Calculate total reach (Likes + Retweets) for each verified user
reach_df = verified_posts_df.groupBy("Username").agg(
    _sum(col("Likes") + col("Retweets")).alias("TotalReach")
)

# Get the top 5 verified users by total reach
top_verified = reach_df.orderBy(col("TotalReach").desc()).limit(5)

# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)
