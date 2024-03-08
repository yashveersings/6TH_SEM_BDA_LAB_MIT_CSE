import pyspark
import os
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys
os.environ['PYSPARK_PYTHON']=sys.executable
os.environ['PYSPARK_DRIVER_PYTHON']=sys.executable
import numpy as np
import pandas as pd
from pyspark.sql import udf
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType

spark=SparkSession.builder.config("spark.driver.memory","16g").appName("Square").getOrCreate()
df_pd=pd.DataFrame(data={'integers':[1,2,3],'float':[1.0,2.0,3.0],'integer_array':[[1,2,3],[4,5,6],[7,8,9]]})
df=spark.createDataFrame(df_pd)

def square(x):
    return x**2

int_squared=f.udf(lambda z:square(z),IntegerType())
(
    df.select('integers','float',int_squared('integers').alias('integer_squared')).show()
)
