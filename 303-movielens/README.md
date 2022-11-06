# 303 Spark - Movielens

The goal of this lab is to run some analysis on a different dataset, [MovieLens](https://grouplens.org/datasets/movielens/).

- [Spark programming guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [RDD APIs](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/rdd/RDD.html)
- [PairRDD APIs](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/rdd/PairRDDFunctions.html)

This lab's notebook is in the ```material``` folder; the solutions will be released in the same folder.

The cluster configuration should be the same from 301 and 302.

Download the dataset [here](https://big.csr.unibo.it/downloads/bigdata/ml-dataset.zip), unzip it and upload the files to S3.

- ml_movies.csv (<u>movieId</u>:Long, title:String, genres:String) 
    - genres are separated by pipelines  (e.g., "comedy|drama|action")
    - each movie is associated with many ratings

- ml_ratings.csv (<u>userId</u>:Long, <u>movieId</u>:Long, rating:Double, year:Int)
    - each rating is associated with many tags
- ml_tags.csv (<u>userId</u>:Long, <u>movieId</u>:Long, <u>tag</u>:String, year:Int) 