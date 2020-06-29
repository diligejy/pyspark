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
    
    def doGroupBy(self, sc):
        rdd1 = sc.parallelize(range(1, 11))
        rdd2 = rdd1.groupBy(lambda v : "even" if v % 2 == 0 else "odd")
        for x in rdd2.collect():
            print(x[0], list(x[1]))

    def doGroupByKey(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c", "b", "c"]).map(lambda v : (v, 1))
        rdd2 = rdd1.groupByKey()
        for x in rdd2.collect():
            print(x[0], list(x[1]))

    def doCoGroup(self, sc):
        rdd1 = sc.parallelize([("k1", "v1"), ("k2", "v2"), ("k1", "v3")])
        rdd2 = sc.parallelize([("k1", "v4")])
        result = rdd1.cogroup(rdd2)
        for x in result.collect():
            print(x[0], list(x[1][0]), list(x[1][1]))
    
    def doDistinct(self, sc):
        rdd = sc.parallelize([1, 2, 3, 1, 2, 3, 1, 2, 3])
        result = rdd.distinct()
        print(result.collect())
    
    def doCartesian(self, sc):
        rdd1 = sc.parallelize([1, 2, 3])
        rdd2 = sc.parallelize(["a", "b", "c"])
        result = rdd1.cartesian(rdd2)
        print(result.collect())

    def doSubtract(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c", "d", "e"])
        rdd2 = sc.parallelize(["d", "e"])
        result = rdd1.subtract(rdd2)
        print(result.collect())

    def doUnion(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c"])
        rdd2 = sc.parallelize(["d", "e", "f"])
        result = rdd1.union(rdd2)
        print(result.collect())
    
    def doIntersection(self, sc):
        rdd1 = sc.parallelize(["a", "a", "b", "c"])
        rdd2 = sc.parallelize(["a", "a", "c", "c"])
        result = rdd1.intersection(rdd2)
        print(result.collect())
    
    def doJoin(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c", "d", "e"]).map(lambda v : (v, 1))
        rdd2 = sc.parallelize(["b", "c"]).map(lambda v : (v, 2))
        result = rdd1.join(rdd2)
        print(result.collect())

    def doLeftRightOuterJoin(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c"]).map(lambda v : (v, 1))
        rdd2 = sc.parallelize(["b", "c"]).map(lambda v : (v, 2))
        result1 = rdd1.leftOuterJoin(rdd2)
        result2 = rdd1.rightOuterJoin(rdd2)
        print("Left : %s" % result1.collect())
        print("Right : %s" % result2.collect())

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
    #obj.doZip(sc)
    #obj.doGroupBy(sc)
    #obj.doGroupByKey(sc)
    #obj.doCoGroup(sc)
    #obj.doDistinct(sc)
    #obj.doCartesian(sc)
    #obj.doSubtract(sc)
    #obj.doUnion(sc)
    #obj.doIntersection(sc)
    #obj.doJoin(sc)
    obj.doLeftRightOuterJoin(sc)