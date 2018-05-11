import sys
from pyspark import SparkContext

if __name__ == "__main__":
    file = sys.argv[1] #raw train file

    sc = SparkContext(appName="demo1")
    data_uc = sc.textFile(file).map(lambda line: line.upper())
    data_filt = data_uc.filter(lambda line: line.startswith("T"))
    #data_uc...
    data_filt.saveAsTextFile("demo_T_output5")

    sc.stop()
