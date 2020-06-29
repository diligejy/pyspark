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
    
    def doMapPartitionsWithIndex(self, sc):
    
        # increaseWithIndex
        def increaseWithIndex(idx, numbers):
            for i in numbers:
                if (idx == 1):
                    yield i + 1
    
        rdd1 = sc.parallelize(range(1, 11), 3)
        rdd2 = rdd1.mapPartitionsWithIndex(increaseWithIndex)
        print(rdd2.collect())
    
    def doMapValues(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c"])
        # (키, 값) 쌍으로 구성된 RDD를 생성
        rdd2 = rdd1.map(lambda v : (v, 1))
        rdd3 = rdd2.mapValues(lambda i : i + 1)
        print(rdd3.collect())

    def doFlatMapValues(self, sc):
        rdd1 = sc.parallelize([(1, "a, b"), (2, "a, c"), (1, "d, e")])
        rdd2 = rdd1.flatMapValues(lambda s : s.split(","))
        print(rdd2.collect())

    def doZip(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c"])
        rdd2 = sc.parallelize([1, 2, 3])
        result = rdd1.zip(rdd2)
        print(result.collect())

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.driver.host", "127.0.0.1")
    sc = SparkContext(master="local[*]", appName="Practice", conf = conf)
    obj = Practice()
    
    #obj.doFlatMap(sc)
    #obj.doMapPartitions(sc)
    #obj.doMapPartitionsWithIndex(sc)
    #obj.doMapValues(sc)
    #obj.doFlatMapValues(sc)
    obj.doZip(sc)