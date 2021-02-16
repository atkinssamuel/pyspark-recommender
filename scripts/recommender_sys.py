from pyspark.sql import SparkSession


if __name__ == "__main__":
    movies_path = "data/movies.csv"

    spark = SparkSession.builder.appName("recommender_sys").getOrCreate()
    movies = spark.read.text(movies_path)

    print(movies.take(5))

    exit()