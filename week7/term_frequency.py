import sys
import json
from pyspark import SparkContext

##./bin/spark-submit --master local[8] --driver-memory 2G term_frequency.py ads_0502.txt
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
    result['adid_terms'] = items[0]
    result['count'] = items[1]
    return json.dumps(result)

if __name__ == "__main__":
    adfile = sys.argv[1] #raw ads data
    sc = SparkContext(appName="TF_Features")
    #[5000_dha,5000_garden,..]
    #(5000_dha,1), (5000_garden,1),...
    data = sc.textFile(adfile).flatMap(lambda line: get_adid_terms(line)).map(lambda w: (w,1)).reduceByKey(lambda v1,v2: v1 + v2).map(generate_json)
    data.saveAsTextFile("/Users/jiayangan/project/SearchAds/data/log/TF11")
    sc.stop()
