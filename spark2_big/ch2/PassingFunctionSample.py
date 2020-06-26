from pyspark import SparkContext, SparkConf


class PassingFunctionSample():

    def add(self, i):
        return i + 1

    def runMapSample1(self, sc):
        rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        # rdd2 = rdd1.map(self.add1) => 잘못된 방법. 'self'전달하고 있음
        rdd2 = rdd1.map(add2) # 이렇게 처리
        print(', '.join(str(i) for i in rdd2.collect()))


if __name__ == '__main__':

    def add2(i):
        return i + 1
        
    conf = SparkConf()
    sc = SparkContext(master = 'local', appName = 'PassingFunctionSample', conf = conf)
    obj = PassingFunctionSample()
    obj.runMapSample1(sc)
    sc.stop()