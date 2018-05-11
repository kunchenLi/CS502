import sys
from pyspark import SparkContext

if __name__ == "__main__":
    file = sys.argv[1] #raw train file

    sc = SparkContext(appName="query_ad_pair_ctr")
    data = sc.textFile(file).map(lambda line: line.strip().strip("\n").encode("utf8", "ignore").split(','))

    #q1, ad1, 1
    #q1, ad1, 1
    #q1, ad1, 0
    #q1, ad1, 0
    query_ad_pair_ctr = data.map(lambda fields: (fields[3] + "_" + fields[4], int(fields[7]))).groupByKey().map(lambda (k, values): (k, sum(values) * 1.0 /len(values)))

    query_ad_pair_ctr.saveAsTextFile("/Users/jiayangan/project/SearchAds/data/log/query_ad_pair_small_ctr")

    sc.stop()
