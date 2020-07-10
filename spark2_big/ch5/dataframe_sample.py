import collections
import time
import numpy as np
import pandas as pd
import pyarrow as pa

from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.image import ImageSchema
from pyspark.sql.types import LongType, BooleanType
from pyspark.sql.functions import pandas_udf, PandasUDFType

spark = SparkSession \
    .builder \
    .appName("sample") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///home/ubuntu/pyspark/spark2_big/warehouse/") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

def createDataFrame(spark, sc):
    sparkHomeDir = "file:/Users/beginspark/Apps/spark"

    # 1. 외부 데이터소스로부터 데이터프레임 생성
    df1 = spark.read.json(sparkHomeDir + "/examples/src/main/resources/people.json")
    df2 = spark.read.parquet(sparkHomeDir + "/examples/src/main/resources/users.parquet")
    df3 = spark.read.text(sparkHomeDir + "/examples/src/main/resources/people.txt")

    row1 = Row(name="hayoon", age = 7, job = "student")
    row2 = Row(name="sunwoo", age = 13, job = "student")
    row3 = Row(name="hajoo", age = 5, job = "kindergartener")
    row4 = Row(name="jinwoo", age = 13, job = "student")

    rdd = spark.sparkContext.parallelize(data)
    df5 = spark.createDataFrame(data)

    # 4. 스키마 지정을 통한 데이터프레임 생성(ex5-23)
    sf1 = StructField("name", StringType(), True)
    sf2 = StructField("age", IntegerType(), True)
    sf3 = StructField("job", StringType(), True)
    schema = StructType([sf1, sf2, sf3])
    r1 = ("hayoon", 7, "student")
    r2 = ("sunwoo", 13, "student")
    r3 = ("hajoo", 5, "kindergartener")
    r4 = ("jinwoo", 13, "student")
    rows = [r1, r2, r3, r4]
    df6 = spark.createDataFrame(rows, schema)