import sys
import json
from pyspark import SparkContext

##./bin/spark-submit --master local[8] --driver-memory 2G document_frequency.py ads_0502.txt
def get_term(line):
    entry = line.split('_')
    return entry[1]

def get_adid_terms(line):
    entry = json.loads(line.strip())
    ad_id = entry['adId']
    adid_terms = []
    #print entry['keyWords']
    for term in entry['keyWords']:
        val = str(ad_id) + "_" + term
        adid_terms.append(val)
    return adid_terms

def generate_json(items):
    result = {}
    result['term'] = items[0]
    result['doc_freq'] = items[1]
    return json.dumps(result)

if __name__ == "__main__":
    adfile = sys.argv[1] #raw ads data
    sc = SparkContext(appName="DF_Features")
    #[1111_makeup, 2311_makeup,2311_makeup, 987_makeup, 433_cosmetic, 867_cosmetic] => #[1111_makeup,2311_makeup, 987_makeup, 433_cosmetic, 867_cosmetic]
    #(makeup , 1), (makeup , 1), (makeup , 1), (cosmetic, 1), (cosmetic, 1)
    data = sc.textFile(adfile).flatMap(lambda line: get_adid_terms(line)).distinct().map(lambda w: (get_term(w),1)).reduceByKey(lambda v1,v2: v1 + v2).map(generate_json)
    data.saveAsTextFile("/Users/jiayangan/project/SearchAds/data/log/DF11")
    sc.stop()
