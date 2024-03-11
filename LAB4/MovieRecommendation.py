import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os
import sys
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

os.environ['PYSPARK_PYTHON']=sys.executable
os.environ['PYSPARK_DRIVER_PYTHON']=sys.executable

spark = SparkSession.builder \
    .appName("ALS Example") \
    .config("spark.executor.memory", "16g") \
    .config("spark.driver.memory", "16g") \
    .config("spark.memory.offHeap.enabled", True) \
    .config("spark.memory.offHeap.size", "16g") \
    .getOrCreate()



path=r"C:\Users\LENOVO\Desktop\SUB 6TH SEM\BDA\LAB\LAB4\ratings.csv"
data=spark.read.csv(path,header=True, inferSchema=True)
data=data.select('userId','movieID','rating').cache()

indexers=[
    StringIndexer(inputCol=column,outputCol=column+"_index").fit(data)
    for column in ['userId','movieID']
    ]

pipeline=Pipeline(stages=indexers)
ratings_indexed=pipeline.fit(data).transform(data)

ratings_indexed.show()

train,test=ratings_indexed.randomSplit([8.0,2.0])

als=ALS(userCol='userId_index',itemCol='movieID_index',ratingCol='rating', maxIter=10,regParam=0.01,rank=10)
eval=RegressionEvaluator(metricName="rmse",labelCol="rating",predictionCol="prediction")
model=als.fit(train)
prediction=model.transform(test)
prediction.show(10,False)

user1=test.filter(test['userId_index']==1).select('userId','movieID','rating','userId_index','movieID_index')
reccomendation=model.transform(user1)
reccomendation.show()

res=eval.evaluate(prediction)
print(res)