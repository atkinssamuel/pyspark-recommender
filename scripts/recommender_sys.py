from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit, CrossValidator
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField
from pyspark.sql import functions as F

import numpy as np
import pandas as pd


def get_accuracy(line):
    if line[0] <= 0.5:
        return (1, 1)
    return (0, 1)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("recommender_sys").getOrCreate()
    sc = spark.sparkContext

    schema = StructType([StructField("movieId", IntegerType(), True),
                         StructField("rating", IntegerType(), True),
                         StructField("userId", IntegerType(), True)])

    df = spark.read.option("header", "true").csv("data/movies.csv", schema=schema)

    question = 1
    if question == 1:
        df.describe().show()
        df.groupBy("movieId").mean("rating").sort(F.col("avg(rating)").desc()).show(10)
        df.groupBy("userId").mean("rating").sort(F.col("avg(rating)").desc()).show(10)
    elif question == 2:
        split_list = [[0.75, 0.25], [0.8, 0.2]]
        for split in split_list:
            training, test = df.randomSplit(split)
            als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
            reg_evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

            model = als.fit(training)
            predictions = model.transform(test)
            RMSE = reg_evaluator.evaluate(predictions)
            pre_accuracy = predictions.withColumn("accuracy", F.abs(predictions["rating"] - predictions["prediction"])) \
                .select("accuracy").rdd \
                .map(get_accuracy) \
                .reduceByKey(lambda v1, v2: v1 + v2) \
                .collect()
            accuracy = pre_accuracy[0][1] / (pre_accuracy[0][1] + pre_accuracy[1][1])
            print(f"RMSE = {round(RMSE, 3)} & Accuracy = {round(accuracy * 100, 2)}% with {split} "
                  f"Train/Test split\nPrediction Summary:")
            predictions.describe().show()
            predictions.select("prediction").show()
    elif question == 3:
        split_list = [[0.75, 0.25], [0.8, 0.2]]
        for split in split_list:
            for metric in ["rmse", "mse"]:
                training, test = df.randomSplit(split)
                als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
                reg_evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
                model = als.fit(training)
                predictions = model.transform(test)
                metric_res = reg_evaluator.evaluate(predictions)

                print(f"{metric.upper()} = {round(metric_res, 3)} with {split} Train/Test split\n")
    elif question == 4 or question == 5:
        split_list = [[0.75, 0.25], [0.8, 0.2]]
        for split in split_list:
            training, test = df.randomSplit(split)
            als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
            reg_evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

            parameters = ParamGridBuilder() \
                .addGrid(als.rank, [5, 10, 20, 40, 80])\
                .addGrid(als.regParam, [0.1, 0.01, 0.001])\
                .addGrid(als.alpha, [2.0, 3.0])\
                .build()

            # simple_parameters = ParamGridBuilder() \
            #     .addGrid(als.rank, [5]).addGrid(als.regParam, [0.1]).addGrid(als.alpha, [2.0]).build()

            cross_validator = CrossValidator(estimator=als, estimatorParamMaps=parameters, evaluator=reg_evaluator,
                                             numFolds=3)

            cv_model = cross_validator.fit(training)

            predictions = cv_model.bestModel.transform(test)

            metric_res = reg_evaluator.evaluate(predictions)

            print(f"Optimal Model:\n"
                  f"rank = {cv_model.bestModel.rank}\n"
                  f"RMSE = {round(metric_res, 3)}\n"
                  f"Train/Test split = {split}\n")

    if question == 5:
        movies_pd = pd.read_csv("data/movies.csv")
        user_11_rated = set(movies_pd[movies_pd["userId"] == 11]["movieId"])
        movieIds_11 = movies_pd[~movies_pd["movieId"].isin(user_11_rated)]["movieId"].unique()

        user_23_rated = set(movies_pd[movies_pd["userId"] == 23]["movieId"])
        movieIds_23 = movies_pd[~movies_pd["movieId"].isin(user_23_rated)]["movieId"].unique()

        id_11 = np.ones(len(movieIds_11)) * 11
        id_23 = np.ones(len(movieIds_23)) * 23

        df_11_pd = pd.DataFrame({"movieId": movieIds_11, "userId": id_11})
        df_11 = spark.createDataFrame(df_11_pd)
        df_11_pred = cv_model.bestModel.transform(df_11).sort(F.col("prediction").desc()).show(15)

        df_23_pd = pd.DataFrame({"movieId": movieIds_23, "userId": id_23})
        df_23 = spark.createDataFrame(df_23_pd)
        df_23_pred = cv_model.bestModel.transform(df_23).sort(F.col("prediction").desc()).show(15)

