import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import os
import sys
import pandas as pd

os.environ["PYSPARK_PYTHON"]=sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"]=sys.executable

spark=SparkSession.builder.config("spark_driver_memory","16g").appName("wordCount").getOrCreate()
sc=spark.sparkContext

path=r"C:\Users\LENOVO\Desktop\SUB 6TH SEM\BDA\LAB\LAB2\sentence.txt"
sentence=sc.textFile(path)

count=sentence.flatMap(lambda line:line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y)
output=count.collect()

for word,count in output:
    print(word,count)

lines=spark.read.text(path)
words=lines.withColumn('word',f.explode(f.split(f.col('value')," "))).groupBy('word').count().sort('count',ascending=False)
words.show()
