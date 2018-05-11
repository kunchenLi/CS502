import os
import sys
import json
import random
from sets import Set
import time

def window(seq, n=2):
    "Returns a sliding window (of width n) over data from the iterable"
    "   s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   "
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result

def ngrams(tokens, size):
    for i in xrange(size):
        for t in window(tokens, i+1):
            yield " ".join(t)

def calculate_relevance_score(query, keywords):
    keyword_set = Set()
    for keyword in keywords:
        keyword_set.add(keyword)

    count_matched = 0
    token_list = query.split(" ")
    for token in token_list:
        if token in keyword_set:
            count_matched += 1

    score = count_matched * 1.0 / len(keyword_set)
    print "relevance_score",score
    return score

if __name__ == "__main__":
    ad_input_file = sys.argv[1]
    query_camp_ad_file = sys.argv[2]
    campaign_weight_file = sys.argv[3]
    ad_weight_file = sys.argv[4]
    query_group_id_query_file = sys.argv[5]
    campaignId_category_file = sys.argv[6]
    campaignId_adId_file = sys.argv[7]

    query_camp_ad = {}
    campaign_weight = {}
    ad_weight = {}
    query_group_id_query = {}
    campaignId_category  = {}
    campaignId_adId = {}

    with open(ad_input_file, "r") as lines:
        for line in lines:
            entry = json.loads(line.strip())
            if "category" in entry and "query" in entry and "campaignId" in entry and "query_group_id" in entry and "keyWords" in entry  and "adId" in entry:
                query = entry["query"].lower()
                campaignId = entry["campaignId"]
                adId = entry["adId"]
                query_group_id = entry["query_group_id"]
                keywords =  entry["keyWords"]
                query_ad_category = entry["category"].lower()
                relevance_score = calculate_relevance_score(query, keywords)
                if query_group_id in query_camp_ad:
                    if campaignId in query_camp_ad[query_group_id]:
                        query_camp_ad[query_group_id][campaignId].append(adId)
                        campaignId_adId[campaignId].append(adId)
                        ad_weight[adId] = relevance_score
                    else:
                        query_camp_ad[query_group_id][campaignId] = []
                        query_camp_ad[query_group_id][campaignId].append(adId)
                        campaignId_adId[campaignId] = []
                        campaignId_adId[campaignId].append(adId)
                        ad_weight[adId] = relevance_score
                else:
                    query_camp_ad[query_group_id] = {}
                    query_camp_ad[query_group_id][campaignId] = []
                    query_camp_ad[query_group_id][campaignId].append(adId)
                    campaignId_adId[campaignId] = []
                    campaignId_adId[campaignId].append(adId)
                    ad_weight[adId] = relevance_score

                if query_group_id in query_group_id_query:
                    query_group_id_query[query_group_id][query] = 1
                else:
                    query_group_id_query[query_group_id] = {}

                campaignId_category[campaignId] = query_ad_category



    for query_group_id in query_camp_ad:
        total_relevance_cross_camp = 0.0
        for camp_id in query_camp_ad[query_group_id]:
            total_ad_relevance_per_camp = 0.0
            for ad_id in query_camp_ad[query_group_id][camp_id]:
                total_ad_relevance_per_camp += ad_weight[ad_id]
                print "total_ad_relevance_per_camp:",total_ad_relevance_per_camp

            for ad_id in query_camp_ad[query_group_id][camp_id]:
                if total_ad_relevance_per_camp > 0.0:
                    ad_weight[ad_id] = ad_weight[ad_id] / total_ad_relevance_per_camp

            total_relevance_cross_camp += total_ad_relevance_per_camp
            campaign_weight[camp_id] = total_ad_relevance_per_camp
            print "campaign_weight[camp_id]:",campaign_weight[camp_id]

        for camp_id in query_camp_ad[query_group_id]:
            if total_relevance_cross_camp > 0.0:
                campaign_weight[camp_id] = campaign_weight[camp_id] / total_relevance_cross_camp


    with open(query_camp_ad_file, 'w') as fp:
        json.dump(query_camp_ad, fp)

    with open(campaign_weight_file, 'w') as fp:
        json.dump(campaign_weight, fp)

    with open(ad_weight_file, 'w') as fp:
        json.dump(ad_weight, fp)

    with open(query_group_id_query_file, 'w') as fp:
        json.dump(query_group_id_query, fp)

    with open(campaignId_category_file, 'w') as fp:
        json.dump(campaignId_category, fp)

    with open(campaignId_adId_file, 'w') as fp:
        json.dump(campaignId_adId, fp)
