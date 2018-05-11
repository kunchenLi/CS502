import sys
from pyspark import SparkContext

if __name__ == "__main__":
    file = sys.argv[1] #raw train file

    sc = SparkContext(appName="demo3")
    #data = sc.textFile(file).flatMap(lambda line: line.upper().split(' ')).map(lambda w: (w,1)).reduceByKey(lambda v1,v2: v1 + v2)
    data = sc.textFile(file).flatMap(lambda line: line.upper().split(' ')).map(lambda w: (w,1)).groupByKey().map(lambda (k, values): (k, sum(values)))

    data.saveAsTextFile("demo3_output_groupbykey1")
    sc.stop()
