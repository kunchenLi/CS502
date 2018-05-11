import os
import sys
import json
import random
import re
from sets import Set

from nltk.corpus import stopwords
from nltk.tokenize import wordpunct_tokenize
from nltk.stem.porter import PorterStemmer

stop_words = set(stopwords.words('english'))
print "length of default stop_words:", len(stop_words)
print "default stop_words:", stop_words
stop_words.update(['.', ',', '"', "'", '?', '$', '!', ':', ';', '(', ')', '[', ']', '{', '}', '&','/','...','-','--', '+','*','|',"),","**"])
print "length of updated stop_words:", len(stop_words)

porter = PorterStemmer()


def cleanData(input) :
    #remove stop words
    list_of_tokens = [i.lower() for i in wordpunct_tokenize(input) if i.lower() not in stop_words ]

    new_tokens = []
    for t in list_of_tokens:
        if t.isdigit() == False and len(t) > 1:
            new_tokens.append(t)
    return new_tokens

if __name__ == "__main__":
    #
    s = "Good muffins cost $3.88\nin New York.  Please buy me\ntwo of them.\n\nThanks. U-S-A"
    print s
    tokens = wordpunct_tokenize(s)
    print tokens
    input_file = sys.argv[1] #ads data
    word2vec_training_file = sys.argv[2]

    word2vec_training = open(word2vec_training_file, "w")

    with open(input_file, "r") as lines:
        for line in lines:
            entry = json.loads(line.strip())
            if  "title" in entry and "category" in entry and "query" in entry:
                    title = entry["title"].lower().encode('utf-8')
                    query = entry["query"].lower().encode('utf-8')
                    category = ""
                    if entry["category"] is not None:
                      category = entry["category"].lower().encode('utf-8')


                    query_tokens = cleanData(query)

                    #remove number from text
                    # new_query_tokens = []
                    # for q in query_tokens:
                    #     if q.isdigit() == False and len(q) > 1:
                    #         new_query_tokens.append(q)

                    #new_title_tokens = []
                    #Nordic Naturals - Prenatal DHA - 180 ct
                    #Rainbow Light Prenatal DHA, 60 Softgels (60 x 2)
                    title_tokens = cleanData(title)

                    if category != "":
                        category_tokens = cleanData(category)

                    # for t in title_tokens:
                    #     if t.isdigit() == False and len(t) > 1:
                    #         new_title_tokens.append(t)

                    queryStr = " ".join(query_tokens)
                    titleStr = " ".join(title_tokens)
                    categoryStr = " ".join(category_tokens)

                    word2vec_training.write(queryStr)
                    word2vec_training.write(" ")
                    word2vec_training.write(titleStr)
                    word2vec_training.write(" ")
                    word2vec_training.write(queryStr)

                    if categoryStr != "":
                        word2vec_training.write(" ")
                        word2vec_training.write(categoryStr)
                        word2vec_training.write(" ")
                        word2vec_training.write(titleStr)
                        word2vec_training.write(" ")
                        word2vec_training.write(categoryStr)
                    word2vec_training.write('\n')

    word2vec_training.close()
