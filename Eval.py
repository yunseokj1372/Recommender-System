from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.evaluation import RankingMetrics

import pandas as pd

from pyspark import SparkContext

sc = SparkContext("local", "First App")

spark = SparkSession.builder.appName('popularity').getOrCreate()
ratings_train = spark.read.option("header",True).parquet('val_large_set.parquet')

df = ratings_train.toPandas()

user_num = len(df.groupby('userId')['userId'])


user_movie = list(df.groupby('userId')['movieId'].apply(list))

popularity = spark.read.option("header",True).parquet('train_large_popularity.parquet')


pop = popularity.toPandas()


pop_movie = [list(pop['movieId'])]*user_num


inp = list(zip(user_movie,pop_movie))

rdd = sc.parallelize(inp)

metrics = RankingMetrics(rdd)
print("Precision 100:", metrics.precisionAt(100))
print("MAP 100:",metrics.meanAveragePrecisionAt(100))
print("NDCG 100:", metrics.ndcgAt(100))
