# -*- coding: utf-8 -*- 

from pyspark import SparkContext, SparkConf



class Practice():

    def doFlatMap(self, sc):
        rdd1 = sc.parallelize(["apple,orange", "grape,apple,mango", "blueberry, tomato, orange"])
        rdd2 = rdd1.flatMap(lambda s : s.split(","))
        print(rdd2.collect())


    
    def doMapPartitions(self, sc):
        
        # increase
        def increase(numbers):
            print("DB 연결!!!")
            return (i + 1 for i in numbers)

        rdd1 = sc.parallelize(range(1, 11))
        rdd2 = rdd1.mapPartitions(increase)
        print(rdd2.collect())
    


if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.driver.host", "127.0.0.1")
    sc = SparkContext(master="local[*]", appName="Practice", conf = conf)
    obj = Practice()
    
    obj.doMapPartitions(sc)
    
    # obj.doFlatMap(sc)
