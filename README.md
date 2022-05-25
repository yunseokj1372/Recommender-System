# Recommender-System

Data:
1. ml-latest-small : 100,000 ratings and 3,600 tag applications applied to 9,000 movies by 600 users.
2. ml-ltest : 27,000,000 ratings and 1,100,000 tag applications applied to 58,000 movies by 280,000 users.

Data can be obtained from:
url : https://grouplens.org/datasets/movielens/latest/


Primary Dataset: ratings.csv



A.

Partition Process:
1. User-based partition: train, validation, test set.
2. Sending past data of train and validation set back to training set

Method: PySpark SQL
Code: Partition_process.py
Results: stored as parquet files in Result Folder


B.

Baseline Model: 
Popularity : top 100 movies of every user based on each movie's average movie rating

Method: PySpark SQL
Code: BaselineModel.py


C.

Latent Factor Model:
1. Collaborative Filtering: PySpark SQL ALS model
2. Lenskit

Code: LatentFactorModel.py, Lenskit.ipynb
