#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py <student_netID>
'''
#Use getpass to obtain user netID
import getpass

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def main(spark, netID):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    netID : string, netID of student to find files in HDFS
    '''

    # Load the boats.txt and sailors.json data into DataFrame
    ratings = spark.read.csv(f'hdfs:/user/yl7143/movielens/ml-latest-small/ratings.csv', header=True,
                            schema='userId INT, movieId INT, rating FLOAT, timestamp INT')
    
    ratings.createOrReplaceTempView('ratings')
    
    user = spark.sql('select userId from ratings group by userId')
    
   
    
    train_user, testval_user = user.randomSplit([0.6, 0.4], seed=13)
    
    
    user.createOrReplaceTempView('user')
    train_user.createOrReplaceTempView('train_user')
    testval_user.createOrReplaceTempView('testval_user')
    

    train_set = spark.sql('select ratings.userId, ratings.movieId, ratings.rating, ratings.timestamp from ratings join train_user on ratings.userId = train_user.userId')
    
    train_set.createOrReplaceTempView('train_set')
    

    
    
    testval_set = spark.sql('select ratings.userId, ratings.movieId, ratings.rating, ratings.timestamp from ratings join testval_user on ratings.userId = testval_user.userId')
    
    testval_set.createOrReplaceTempView('testval_set')

    
    
    val_user, test_user = testval_user.randomSplit([0.5, 0.5])
    
    val_user.createOrReplaceTempView('val_user')
    test_user.createOrReplaceTempView('test_user')
    
    
    val_set = spark.sql('select testval_set.userId, testval_set.movieId, testval_set.rating, testval_set.timestamp from testval_set join val_user on testval_set.userId = val_user.userId')

    
    val_set.createOrReplaceTempView('val_set')
    
    test_set = spark.sql('select testval_set.userId, testval_set.movieId, testval_set.rating, testval_set.timestamp from testval_set join test_user on testval_set.userId = test_user.userId')
    
    test_set.createOrReplaceTempView('test_set')

    
    temp1_set = spark.sql('select userId, timestamp, movieId, rating, percent_rank() OVER (PARTITION BY userId ORDER BY timestamp) as percent from val_set')
    
    temp2_set = spark.sql('select userId, timestamp, movieId, rating, percent_rank() OVER (PARTITION BY userId ORDER BY timestamp) as percent from test_set')
    
    temp1_set.createOrReplaceTempView('temp1_set')
    temp2_set.createOrReplaceTempView('temp2_set')
    
    train1_sending = spark.sql('select userId, movieId, rating, timestamp from temp1_set where percent <= 0.15')
    train1_sending.createOrReplaceTempView('train1_sending')
    
    
    train2_sending = spark.sql('select userId, movieId, rating, timestamp from temp2_set where percent <= 0.15')
    train2_sending.createOrReplaceTempView('train2_sending')
    
    final_val_set = spark.sql('select userId, timestamp, movieId, rating from temp1_set where percent > 0.15')
    final_test_set = spark.sql('select userId, timestamp, movieId, rating from temp2_set where percent > 0.15')
    
   
    final_val_set.createOrReplaceTempView('final_val_set')
    final_test_set.createOrReplaceTempView('final_test_set')
    
    
    comb1 = train_set.union(train1_sending)
    comb1.createOrReplaceTempView('comb1')
    
    train_combined = comb1.union(train2_sending)
    train_combined.createOrReplaceTempView('train_combined')
    
    
    train_combined.write.mode('overwrite').parquet('train_combined_small_set.parquet')
    train_set.write.mode('overwrite').parquet('train_small_set.parquet')
    train1_sending.write.mode('overwrite').parquet('train1_sending_small_set.parquet')
    train2_sending.write.mode('overwrite').parquet('train2_sending_small_set.parquet')
    final_val_set.write.mode('overwrite').parquet('val_small_set.parquet')
    final_test_set.write.mode('overwrite').parquet('test_small_set.parquet')
    
    #train_combined.write.mode('overwrite').parquet('train_combined_large_set.parquet')
    #train_set.write.mode('overwrite').parquet('train_large_set.parquet')
    #train1_sending.write.mode('overwrite').parquet('train1_sending_large_set.parquet')
    #train2_sending.write.mode('overwrite').parquet('train2_sending_large_set.parquet')
    #final_val_set.write.mode('overwrite').parquet('val_large_set.parquet')
    #final_test_set.write.mode('overwrite').parquet('test_large_set.parquet')





    
    
    

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('partitioning').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark, netID)
