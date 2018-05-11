import sys
import time
import json
from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec

training_file = sys.argv[1]
synonyms_data_file = sys.argv[2]
print  training_file
sc = SparkContext(appName="word2vec")
training_data = sc.textFile(training_file).map(lambda line: line.encode("utf8", "ignore").split(" "))

word2vec = Word2Vec()
#millis = int(round(time.time() * 1000))
model = word2vec.setMinCount(5).setVectorSize(100).setSeed(2017).setWindowSize(4).fit(training_data)
#model = word2vec.setVectorSize(10).setSeed(2017).fit(inp)
#model = word2vec.setLearningRate(0.01).setWindowSize(4).setMinCount(5).setVectorSize(20).setSeed(2017).fit(training_data)

#model = word2vec.setMinCount(5).setVectorSize(10).setSeed(2017).fit(inp)

vec = model.getVectors()
synonyms_data = open(synonyms_data_file, "w")

print "len of vec", len(vec)
for word in vec.keys():
    synonyms = model.findSynonyms(word, 10)
    entry = {}
    entry["word"] = word
    synon_list = []
    for synonym, cosine_distance in synonyms:
        synon_list.append(synonym)
    entry["synonyms"] = synon_list
    synonyms_data.write(json.dumps(entry))
    synonyms_data.write('\n')

synonyms_data.close()


#print vec
test_data = [ "furniture", "shaver", "toddler", "sport","xbox", "led","organizer"]
for w in test_data:
    synonyms = model.findSynonyms(w, 10)
    print "synonyms of ",w
    for word, cosine_distance in synonyms:
        print("{}: {}".format(word, cosine_distance))

model.save(sc, "../data/model/word2vec_0416")
sc.stop()
