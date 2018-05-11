import sys
from pyspark import SparkContext
# given the list of neighbors for a page and that page's rank, calculate
# what that page contributes to the rank of its neighbors
def computeContribs(neighbors, rank):
    for neighbor in neighbors:
        yield(neighbor, rank/len(neighbors))


# read in a file of page links (format: url1 url2)
linkfile="pagelinks.txt"
sc = SparkContext(appName="pagerank")
links = sc.textFile(linkfile).map(lambda line: line.split()).map(lambda pages: (pages[0],pages[1])).distinct().groupByKey().persist() # filter out duplicates
#groupByKey => adjeacent list: (page3, [page1,page4])

# set initial page ranks to 1.0
ranks = links.map(lambda (page,neighbors): (page,1.0))

# number of iterations
n = 20
d = 0.85
# for n iterations, calculate new page ranks based on neighbor contribibutios
for x in xrange(n):
  contribs=links.join(ranks).flatMap(lambda (page,(neighbors,rank)):computeContribs(neighbors,rank))

    #page1, 0.5
    #page2, 0.7
    #page3, 0.2
    #page1, 0.4
    #page3, 0.5

  ranks=contribs\
    .reduceByKey(lambda v1,v2: v1+v2)\
    .map(lambda (page,contrib): \
         (page,contrib * d + (1 - d)))
  print "Iteration ",x
  for pair in ranks.take(4): print pair

sc.stop()
