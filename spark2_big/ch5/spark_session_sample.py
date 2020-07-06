from pyspark.sql import SparkSession
spark = SparkSession\
                .builder\
                .appName("sample")\
                .master("local[*]")\
                .getOrCreate()