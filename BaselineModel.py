#!/usr/bin/env python
# -*- coding: utf-8 -*-

#Use getpass to obtain user netID
import getpass

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.mllib.evaluation import RankingMetrics



def main(spark, netID):
    '''
    Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    netID : string, netID of student to find files in HDFS
    '''
    ratings = spark.read.parquet(f'hdfs:/user/{netID}/train_combined_small_set.parquet')
    #ratings = spark.read.parquet(f'hdfs:/user/{netID}/train_combined_large_set.parquet')
    ratings.createOrReplaceTempView('ratings')
    avg_scores = spark.sql('select ratings.movieId, avg(ratings.rating) as average from ratings group by ratings.movieId order by average desc LIMIT 100')
    avg_scores.createOrReplaceTempView('avg_scores')
    avg_scores.show()
    #print(avg_scores)
    #avg_scores.write.mode('overwrite').parquet('hdfs:/user/yl7143/train_large_popularity.parquet')

    ratings_val = spark.read.parquet(f'hdfs:/user/{netID}/val_small_set.parquet') # TODO timestamep type
    #ratings_val = spark.read.parquet(f'hdfs:/user/{netID}/val_large_set.parquet')
    ratings_val.createOrReplaceTempView('ratings_val')
    avg_scores_val = spark.sql('SELECT ratings_val.movieId, AVG(ratings_val.rating) as average from ratings_val group by ratings_val.movieId order by average desc LIMIT 100')
    avg_scores_val.createOrReplaceTempView('avg_scores_val')
    avg_scores_val.show()
    #avg_scores_val.write.mode('overwrite').parquet('hdfs:/user/yl7143/val_small_popularity.parquet')

    ratings_test = spark.read.parquet(f'hdfs:/user/{netID}/test_small_set.parquet') # TODO timestamep type
    # ratings_test = spark.read.parquet(f'hdfs:/user/{netID}/test_large_set.parquet')  # TODO timestamep type
    ratings_test.createOrReplaceTempView('ratings_test')
    avg_scores_test = spark.sql('SELECT ratings_test.movieId, AVG(ratings_test.rating) as average FROM ratings_test GROUP BY ratings_test.movieId ORDER BY average desc LIMIT 100')
    avg_scores_test.createOrReplaceTempView('avg_scores_test')
    avg_scores_test.show()


    
    # print('Printing boats inferred schema')
    # boats.printSchema()
    # print('Printing sailors inferred schema')
    # sailors.printSchema()
    # # Why does sailors already have a specified schema?

    # print('Reading boats.txt and specifying schema')
    # boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')

    # print('Printing boats with specified schema')
    # boats.printSchema()

    # # Give the dataframe a temporary view so we can run SQL queries
    # boats.createOrReplaceTempView('boats')
    # sailors.createOrReplaceTempView('sailors')
    # # Construct a query
    # print('Example 1: Executing SELECT count(*) FROM boats with SparkSQL')
    # query = spark.sql('SELECT count(*) FROM boats')

    # # Print the results to the console
    # query.show()

    

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('popularity').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark, netID)
