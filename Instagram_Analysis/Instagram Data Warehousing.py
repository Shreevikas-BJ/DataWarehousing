# Databricks notebook source
# Defining Storage Credentials
storage_account_name = "instagramstorage"
container_name = "instagramdata"
storage_account_key = "J4rBhGFEkzYSleCbqnsPgKQV9rKW4THdPC7euxwXWmvT7gq72/E1opAW5d8L3XxCV7QXdizpgD84+AStVbEnKg=="

# Mounting Azure Blob Storage
dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point="/mnt/instagramstorage",
    extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
)

# Verifying Mount
display(dbutils.fs.ls("/mnt/instagramstorage"))

# COMMAND ----------

# Writing raw-data into staging delta tables
df_comments = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/instagramstorage/comments.csv")

df_comments.write.format("delta").mode("overwrite").saveAsTable("comments")

df_follows = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/instagramstorage/follows.csv")

df_follows.write.format("delta").mode("overwrite").saveAsTable("follows")

df_likes = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/instagramstorage/likes.csv")

df_likes.write.format("delta").mode("overwrite").saveAsTable("likes")

df_photo_tags = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/instagramstorage/photo_tags.csv")

df_photo_tags.write.format("delta").mode("overwrite").saveAsTable("photo_tags")

df_photos = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/instagramstorage/photos.csv")

df_photos.write.format("delta").mode("overwrite").saveAsTable("photos")

df_tags = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/instagramstorage/tags.csv")

df_tags.write.format("delta").mode("overwrite").saveAsTable("tags")

df_users = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/instagramstorage/users.csv")

df_users.write.format("delta").mode("overwrite").saveAsTable("users")

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id

# Load staging Delta tables with renamed and fixed columns
staging_users = spark.read.format("delta").table("users") \
    .withColumnRenamed("id", "user_id") \
    .withColumnRenamed("created_at", "account_created_date")

staging_photos = spark.read.format("delta").table("photos") \
    .withColumnRenamed("id", "photo_id") \
    .withColumnRenamed("image_url", "photo_url") \
    .withColumnRenamed("created_dat", "upload_date")  # Fix typo

staging_comments = spark.read.format("delta").table("comments") \
    .withColumnRenamed("id", "comment_id") \
    .withColumnRenamed("created_at", "comment_date")

staging_follows = spark.read.format("delta").table("follows") \
    .withColumnRenamed("followee_id", "followed_id") \
    .withColumnRenamed("created_at", "follow_date") \
    .withColumn("follow_id", monotonically_increasing_id())  # Add surrogate key

staging_likes = spark.read.format("delta").table("likes") \
    .withColumnRenamed("created_at", "like_date")

staging_photo_tags = spark.read.format("delta").table("photo_tags")
staging_tags = spark.read.format("delta").table("tags") \
    .withColumnRenamed("id", "tag_id")

# Write transformed data back to Delta tables
staging_users.write.format("delta").mode("overwrite").saveAsTable("staging_users")
staging_photos.write.format("delta").mode("overwrite").saveAsTable("staging_photos")
staging_comments.write.format("delta").mode("overwrite").saveAsTable("staging_comments")
staging_follows.write.format("delta").mode("overwrite").saveAsTable("staging_follows")
staging_likes.write.format("delta").mode("overwrite").saveAsTable("staging_likes")
staging_photo_tags.write.format("delta").mode("overwrite").saveAsTable("staging_photo_tags")
staging_tags.write.format("delta").mode("overwrite").saveAsTable("staging_tags")

print("Schema corrections applied, ready for Fact & Dimension processing!")


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, current_date
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Initialize Spark session
spark = SparkSession.builder.appName("InstagramDW_ETL").getOrCreate()

# Load cleaned staging tables
staging_users = spark.read.format("delta").table("staging_users")
staging_photos = spark.read.format("delta").table("staging_photos")
staging_comments = spark.read.format("delta").table("staging_comments")
staging_follows = spark.read.format("delta").table("staging_follows")
staging_likes = spark.read.format("delta").table("staging_likes")

# Function to apply SCD Type 2 updates
def upsert_scd2(table_name, new_data, key_column, update_columns):
    delta_table_path = f"dbfs:/mnt/instagramstorage/{table_name}"

    if DeltaTable.isDeltaTable(spark, delta_table_path):
        dim_table = DeltaTable.forPath(spark, delta_table_path)

        # Identify updated records
        updates = new_data.alias("stg").join(dim_table.toDF().alias("dim"), key_column, "left") \
            .where(" OR ".join([f"stg.{col} <> dim.{col}" for col in update_columns])) \
            .select("stg.*")

        # Apply SCD Type 2 merge
        dim_table.alias("dim").merge(
            updates.alias("stg"),
            f"dim.{key_column} = stg.{key_column}"
        ).whenMatchedUpdate(set={
            "effective_end_date": current_date()
        }).whenNotMatchedInsert(values={
            "sk_id": lit(None),  
            key_column: f"stg.{key_column}",
            **{col: f"stg.{col}" for col in update_columns},
            "effective_start_date": current_date(),
            "effective_end_date": lit(None)
        }).execute()
    else:
        new_data.withColumn("effective_start_date", current_date()) \
                .withColumn("effective_end_date", lit(None)) \
                .write.format("delta").saveAsTable(table_name)

# 1. Load Dim_User
dim_user_data = staging_users.withColumn("sk_id", row_number().over(Window.orderBy("user_id")))
upsert_scd2("dim_user", dim_user_data, "user_id", ["username", "account_created_date"])

# 2. Load Dim_Photo
dim_photo_data = staging_photos.withColumn("sk_id", row_number().over(Window.orderBy("photo_id")))
upsert_scd2("dim_photo", dim_photo_data, "photo_id", ["user_id", "upload_date", "photo_url"])

# 3. Create Dim_InteractionType (Static)
interaction_types = [(1, "like"), (2, "comment"), (3, "follow")]
df_interaction_types = spark.createDataFrame(interaction_types, ["interaction_type_sk", "interaction_type"])
df_interaction_types.write.format("delta").mode("overwrite").saveAsTable("dim_interaction_type")

# Load dimensions
dim_users = spark.read.format("delta").table("dim_user")
dim_photos = spark.read.format("delta").table("dim_photo")
dim_interaction_types = spark.read.format("delta").table("dim_interaction_type")

# 4. Creating Fact_Interactions
fact_interactions = (
    staging_likes.select("user_id", "photo_id", lit(1).alias("interaction_type_sk"), lit(None).alias("comment_text"), lit(None).alias("followed_user_id"), col("like_date").alias("interaction_time"))
    .union(
        staging_comments.select("user_id", "photo_id", lit(2).alias("interaction_type_sk"), col("comment_text"), lit(None).alias("followed_user_id"), col("comment_date").alias("interaction_time"))
    )
    .union(
        staging_follows.select("follower_id", lit(None).alias("photo_id"), lit(3).alias("interaction_type_sk"), lit(None).alias("comment_text"), col("followed_id").alias("followed_user_id"), col("follow_date").alias("interaction_time"))
    )
)

# Add surrogate keys
fact_interactions = fact_interactions.alias("s") \
    .join(dim_users.alias("u"), col("s.user_id") == col("u.user_id")) \
    .join(dim_photos.alias("p"), col("s.photo_id") == col("p.photo_id"), "left") \
    .join(dim_users.alias("fu"), col("s.followed_user_id") == col("fu.user_id"), "left") \
    .selectExpr("u.sk_id AS user_sk", "p.sk_id AS photo_sk", "s.interaction_type_sk", "s.comment_text", "fu.sk_id AS followed_user_sk", "s.interaction_time")

# Write to Delta
fact_interactions.write.format("delta").mode("overwrite").saveAsTable("fact_interactions")

print("ETL Process Completed: Optimized Dimension and Fact Tables Created Successfully!")


# COMMAND ----------

query2 = """
SELECT 
    p.photo_url, 
    u.username AS uploaded_by, 
    COUNT(f.interaction_type_sk) AS total_likes
FROM fact_interactions f
JOIN dim_photo p ON f.photo_sk = p.sk_id  -- ✅ Fixed: Changed `photo_sk` to `sk_id`
JOIN dim_user u ON p.user_id = u.user_id
WHERE f.interaction_type_sk = (SELECT interaction_type_sk FROM dim_interaction_type WHERE interaction_type = 'like')
GROUP BY p.photo_url, u.username
ORDER BY total_likes DESC
LIMIT 5
"""
df_query2 = spark.sql(query2).toPandas()

# Visualization
import matplotlib.pyplot as plt

plt.figure(figsize=(10,5))
plt.barh(df_query2["photo_url"], df_query2["total_likes"], color="orange")
plt.xlabel("Total Likes")
plt.ylabel("Photo URL")
plt.title("Top 5 Most Liked Photos")
plt.gca().invert_yaxis()
plt.show()


# COMMAND ----------

query5 = """
WITH user_activity AS (
    SELECT 
        u.username, 
        COUNT(f.user_sk) AS total_interactions
    FROM fact_interactions f
    JOIN dim_user u ON f.user_sk = u.sk_id
    GROUP BY u.username
),
user_engagement AS (
    SELECT 
        u.username, 
        COUNT(f.followed_user_sk) AS received_interactions
    FROM fact_interactions f
    JOIN dim_user u ON f.followed_user_sk = u.sk_id
    WHERE f.followed_user_sk IS NOT NULL
    GROUP BY u.username
)
SELECT 
    ua.username, 
    ua.total_interactions, 
    ue.received_interactions,
    (ua.total_interactions + COALESCE(ue.received_interactions, 0)) AS engagement_score
FROM user_activity ua
LEFT JOIN user_engagement ue ON ua.username = ue.username
ORDER BY engagement_score DESC
LIMIT 10
"""
df_query5 = spark.sql(query5).toPandas()

# Visualization
plt.figure(figsize=(10,5))
plt.barh(df_query5["username"], df_query5["engagement_score"], color="darkgreen")
plt.xlabel("Engagement Score")
plt.ylabel("Username")
plt.title("Top 10 Most Engaging Users")
plt.gca().invert_yaxis()
plt.show()


# COMMAND ----------

query4 = """
SELECT 
    u.username, 
    COUNT(f.followed_user_sk) AS total_followers
FROM fact_interactions f
JOIN dim_user u ON f.followed_user_sk = u.sk_id
WHERE f.interaction_type_sk = 3  -- 3 represents "follow"
GROUP BY u.username
ORDER BY total_followers DESC
LIMIT 10
"""
df_query4 = spark.sql(query4).toPandas()

# Visualization
plt.figure(figsize=(10,5))
plt.barh(df_query4["username"], df_query4["total_followers"], color="purple")
plt.xlabel("Total Followers")
plt.ylabel("Username")
plt.title("Top 10 Most Followed Users")
plt.gca().invert_yaxis()
plt.show()


# COMMAND ----------

query_pie = """
SELECT 
    it.interaction_type, 
    COUNT(f.user_sk) AS interaction_count
FROM fact_interactions f
JOIN dim_interaction_type it ON f.interaction_type_sk = it.interaction_type_sk
GROUP BY it.interaction_type
ORDER BY interaction_count DESC
"""
df_pie = spark.sql(query_pie).toPandas()

# Visualization: Pie Chart
import matplotlib.pyplot as plt

plt.figure(figsize=(8, 8))
plt.pie(df_pie["interaction_count"], labels=df_pie["interaction_type"], autopct='%1.1f%%', colors=["blue", "green", "red"])
plt.title("Distribution of Interaction Types (Likes, Comments, Follows)")
plt.show()
