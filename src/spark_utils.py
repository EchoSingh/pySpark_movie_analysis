from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    desc, col, from_unixtime, to_date, hour, date_format,
    stddev, count, avg, explode, split
)
import kagglehub
import os

def get_spark():
    # Spark session initialize karte hain.
    return SparkSession.builder \
        .appName("MovieLensAnalysis") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.network.timeout", "1600s") \
        .getOrCreate()


def load_data(spark):
    # Kaggle ke through dataset download kar rahe hain
    dataset_path = kagglehub.dataset_download("grouplens/movielens-20m-dataset")
    print("Path to dataset files:", dataset_path)
    
    # Data file paths set kar rahe hain
    ratings_file = os.path.join(dataset_path, "rating.csv")
    movies_file = os.path.join(dataset_path, "movie.csv")
    
    # CSV files ko read kar ke DataFrame me convert kar rahe hain
    ratings_df = spark.read.csv(ratings_file, header=True, inferSchema=True)
    movies_df  = spark.read.csv(movies_file, header=True, inferSchema=True)
    
    return ratings_df, movies_df

def task_a_lowest_avg_movie(ratings_df, movies_df):
    ratings_trimmed = ratings_df.select("movieId", "rating")

    avg_ratings_df = ratings_trimmed.groupBy("movieId").agg(avg("rating").alias("avg_rating"))

    lowest_row = avg_ratings_df.orderBy("avg_rating").limit(1).first()

    if not lowest_row:
        return "No valid average ratings found."
    movie_title_row = movies_df.filter(movies_df.movieId == lowest_row.movieId).select("title").first()
    title = movie_title_row.title if movie_title_row else "Unknown Title"

    return f"Lowest rated movie: {title} ({lowest_row.avg_rating:.2f})"



def task_b_top_users(ratings_df):
    # GroupBy userId karke movie count nikalte hain aur top 10 users sort karte hain
    df = (ratings_df.groupBy("userId")
          .agg(count("movieId").alias("num_ratings"))
          .orderBy(desc("num_ratings"))
          .limit(10)
          .toPandas())
    df.index = df.index + 1
    df.index.name = "Rank"
    return df

def task_c_rating_distribution(ratings_df):
    # Rating ke timestamp ko convert karke date column generate karte hain
    ratings_with_date = ratings_df.withColumn("date", to_date(from_unixtime(col("timestamp").cast("bigint"))))
    grouped = ratings_with_date.groupBy("date").agg(count("rating").alias("num_ratings")).orderBy("date")
    return grouped.toPandas()

def task_d_highest_rated_with_min_votes(ratings_df, movies_df, min_votes=50):
    # Har movie ka average rating aur rating count nikalte hain
    avg_df = (ratings_df.groupBy("movieId")
              .agg(avg("rating").alias("avg_rating"), count("rating").alias("num_ratings")))
    # Minimum votes filter (default = 50) lagate hain
    filtered = avg_df.filter(col("num_ratings") >= min_votes)
    top = filtered.orderBy(desc("avg_rating")).limit(10)
    # Movie titles ke sath join karke final data result milta hai
    joined = top.join(movies_df, on="movieId").select("title", "avg_rating", "num_ratings")
    df = joined.toPandas()
    df.index = df.index + 1
    df.index.name = "Rank"
    return df

def task_f_controversial_movies(ratings_df, movies_df, min_ratings=50):
    # Har movie ke liye standard deviation calculate karte hain
    stats_df = (ratings_df.groupBy("movieId")
                .agg(count("*").alias("num_ratings"),
                     avg("rating").alias("avg_rating"),
                     stddev("rating").alias("stddev_rating"))
                .filter(col("num_ratings") >= min_ratings)
                .orderBy(desc("stddev_rating")))
    joined = stats_df.join(movies_df, on="movieId").select("title", "avg_rating", "stddev_rating", "num_ratings")
    df = joined.limit(10).toPandas()
    df.index = df.index + 1
    df.index.name = "Rank"
    return df

def task_h_avg_rating_by_genre(ratings_df, movies_df):
    # Movies ke genres ko explode karke alag-alag genre rows banate hain
    movies_split = movies_df.withColumn("genre", explode(split("genres", "\\|")))
    joined = ratings_df.join(movies_split, on="movieId")
    df = (joined.groupBy("genre")
          .agg(avg("rating").alias("avg_rating"))
          .orderBy("avg_rating", ascending=False)
          .toPandas())
    df.index = df.index + 1
    df.index.name = "Rank"
    return df
