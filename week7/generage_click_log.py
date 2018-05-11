import os
import sys
import json
import random
from sets import Set
import time

#items is dict , key is the data, val is weight
def weighted_sampling(items):
    u = random.uniform(0,1)
    #print "u",u
    cumulative_weight = 0.0
    while 1:
        for key in items:
             cumulative_weight += items[key]
             if u <= cumulative_weight:
                 return key
    return -1


def test_weighted_sampling(query_camp_ad, campaign_weight):
    for query_group_id in query_camp_ad:
        sample_camps_weight = {}
        sample_camps = {}
        for camp_id in query_camp_ad[query_group_id]:
            print "camp_id:",camp_id,",weight:",campaign_weight[camp_id]
            sample_camps_weight[camp_id] = campaign_weight[camp_id]

        total_freq = 0
        for i in range(1000):
            sample_camp_id = weighted_sampling(sample_camps_weight)
            total_freq += 1
            if sample_camp_id in sample_camps:
                sample_camps[sample_camp_id] += 1
            else:
                sample_camps[sample_camp_id] = 1

        for sample_camp_id in sample_camps:
            print "camp_id:",sample_camp_id,",freq distribution:",sample_camps[sample_camp_id] * 1.0 / total_freq

        print "================================="

def random_campaignId(exclusive_campaign_id):
    millis = int(round(time.time() * 1000))
    random.seed(millis)
    size_exclusive_camp_id = len(exclusive_campaign_id)
    index = 0
    if size_exclusive_camp_id > 1 :
        index = random.randint(0,size_exclusive_camp_id - 1)
    sample_camp_id = exclusive_campaign_id[index]
    return sample_camp_id

def random_adId(exclusive_ad_id):
    size_campaignId_adId = len(exclusive_ad_id)
    index_ad = 0
    if size_campaignId_adId > 1:
        index_ad = random.randint(0,size_campaignId_adId - 1)
    sample_ad_id = exclusive_ad_id[index_ad]
    return sample_ad_id

def mismatch_query_categr_ads_category_sampling(query_group_id, query_category, query_camp_ad,campaignId_category,campaignId_adId):
    exclusive_campaign_id = []
    for campignId in campaignId_category:
        if query_category != campaignId_category[campignId]:
            exclusive_campaign_id.append(campignId)

    for camp_id in exclusive_campaign_id:
        if camp_id in query_camp_ad[query_group_id]:
            exclusive_campaign_id.remove(camp_id)

    if len(exclusive_campaign_id) == 0:
        return ()
    sample_camp_id = random_campaignId(exclusive_campaign_id)
    sample_ad_id = random_adId(campaignId_adId[sample_camp_id])

    return (sample_ad_id, sample_camp_id, 0)

def  mismatched_query_campaignId_adId_sampling(query_group_id, camp_id,query_camp_ad,campaignId_adId):
    exclusive_campaign_id = []
    millis = int(round(time.time() * 1000))
    random.seed(millis)
    r = random.randint(0,1)
    query_camp_category_match = 1
    if r == 0:
        #cross camp per query_group_id
        for campignId in query_camp_ad[query_group_id]:
                if campignId != camp_id:
                    exclusive_campaign_id.append(campignId)
    else:
        #cross query group id
        query_camp_category_match = 0
        for q_g_id in query_camp_ad:
            if q_g_id != query_group_id:
                for campignId in query_camp_ad[q_g_id]:
                    exclusive_campaign_id.append(campignId)

    #print exclusive_campaign_id
    if len(exclusive_campaign_id) == 0 :
        return ()
    sample_camp_id = random_campaignId(exclusive_campaign_id)
    sample_ad_id = random_adId(campaignId_adId[sample_camp_id])
    return (sample_ad_id, sample_camp_id, query_camp_category_match)


def lowest_campaignId_adId_weight(query_group_id,query_camp_ad,campaign_weight,ad_weight):
    lowest_camp_weight = 1.0
    lowest_weight_campId = 0
    if len(query_camp_ad[query_group_id]) <= 1:
        return ()
    for campId in query_camp_ad[query_group_id]:
        w = campaign_weight[campId]
        if w < lowest_camp_weight:
            lowest_camp_weight = w
            lowest_weight_campId = campId

    lowest_weight = 1.0
    lowest_weight_adId = 0
    if len(campaignId_adId[lowest_weight_campId]) <= 1:
        return ()
    for adId in campaignId_adId[lowest_weight_campId]:
        w = ad_weight[str(adId)]
        if w < lowest_weight:
            lowest_weight = w
            lowest_weight_adId = adId

    return (lowest_weight_adId, lowest_weight_campId, 1)

#negative sample (no click query)type
#0: mismatched query_categr ads_category 20%
#1: mismatched Query_CampaignId 10%
#2: lowest campaignId weight, lowest adId weight 30%
#3: matched but no click 40%
#return AdId,CampaignId,Ad_category_Query_category(0/1)
def negative_sampling(ip,device_id,ad,query_camp_ad, campaign_weight,ad_weight,campaignId_category,campaignId_adId, negative_type):
    fields = []
    query = ad["query"].lower()
    query_group_id = str(ad["query_group_id"])
    query_category = ad["category"].lower()
    camp_id = str(ad["campaignId"])
    ad_id = str(ad["adId"])
    result = (0,0,0)
    if len(campaignId_adId[camp_id]) <= 1 :
        return fields

    if negative_type == 0:
        result =  mismatch_query_categr_ads_category_sampling(query_group_id, query_category, query_camp_ad, campaignId_category,campaignId_adId)

    if negative_type == 1:
        result = mismatched_query_campaignId_adId_sampling(query_group_id, camp_id, query_camp_ad,campaignId_adId)

    if negative_type == 2:
        result = lowest_campaignId_adId_weight(query_group_id, query_camp_ad, campaign_weight, ad_weight)

    if negative_type == 3:
        result = (ad_id, camp_id, 1,0)

    if len(result) == 0:
        return fields

    if query == "" or str(result[0]) == "" or str(result[1]) == "" or str(result[2]) == "":
        print "invalid fields in negative_sampling",query, str(result[0]),str(result[1]), str(result[2])
        return fields
    sessionId = int(round(time.time() * 1000))
    #Device IP, Device id,Session id,Query,AdId,CampaignId,Ad_category_Query_category(0/1),clicked(0/1)

    fields.append(str(ip))
    fields.append(str(device_id))
    fields.append(str(sessionId))
    fields.append(query)
    fields.append(str(result[0]))
    fields.append(str(result[1]))
    fields.append(str(result[2]))
    fields.append("0")
    return fields


def all_positive_Sampling(ip,device_id,query_group_id,adId_query, query_camp_ad,click_log_output):
    for campId in query_camp_ad[query_group_id]:
        for adId in query_camp_ad[query_group_id][campId]:
                query = adId_query[adId]
                sessionId = int(round(time.time() * 1000))
                #Device IP, Device id,Session id,Query,AdId,CampaignId,clicked(0/1),Ad_category_Query_category(0/1)
                fields = []
                fields.append(str(ip))
                fields.append(str(device_id))
                fields.append(str(sessionId))
                fields.append(query)
                fields.append(str(adId))
                fields.append(str(campId))
                fields.append("1")
                fields.append("1")
                line = ",".join(fields)
                click_log_output.write(line)
                click_log_output.write('\n')

def positive_sampling(ip,device_id,query_group_id, adId_query,query_camp_ad):
    cur_camp_weight =  {}
    cur_ad_weight =  {}
    #print "current query_group_id",query_group_id
    for campId in query_camp_ad[query_group_id]:
        cur_camp_weight[campId] = campaign_weight[campId]

    sample_camp_id = weighted_sampling(cur_camp_weight)
    #print "current sample_camp_id",sample_camp_id

    for adId in query_camp_ad[query_group_id][sample_camp_id]:
        cur_ad_weight[adId] = ad_weight[str(adId)]

    sample_ad_id = weighted_sampling(cur_ad_weight)
    #print "current sample_ad_id",sample_ad_id

    query = adId_query[sample_ad_id]

    sessionId = int(round(time.time() * 1000))
    #Device IP, Device id,Session id,Query,AdId,CampaignId,Ad_category_Query_category(0/1),clicked(0/1)
    fields = []

    if query == "" or str(sample_ad_id) == "" or str(sample_camp_id) == "":
        print "invalid fields in positive_sampling",query, str(sample_ad_id), str(sample_camp_id)
        return fields


    fields.append(str(ip))
    fields.append(str(device_id))
    fields.append(str(sessionId))
    fields.append(query)
    fields.append(str(sample_ad_id))
    fields.append(str(sample_camp_id))
    fields.append("1")
    fields.append("1")
    return fields

def valid(fields):
    if len(fields) < 8 :
        #print "invalid fields",fields
        return False

    return True
if __name__ == "__main__":
    #Device IP, Device id,Session id,Query,AdId,CampaignId,Ad_category_Query_category(0/1),clicked(0/1)
    #use following feature to generate click Log
    #IP, device_id, AdId,QueryCategry_AdsCategory,Query_CampaignId, Query_AdId

    ad_input_file = sys.argv[1]
    user_input_file = sys.argv[2]
    query_ad_input_file = sys.argv[3]
    campaign_weight_input_file = sys.argv[4]
    ad_weight_input_file = sys.argv[5]

    #query_group_id_query_file = sys.argv[6]
    campaignId_category_file = sys.argv[6]
    campaignId_adId_file = sys.argv[7]
    click_log_output_file = sys.argv[8]

    ad_list = []
    adId_query = {}
    query_camp_ad = {}
    campaign_weight = {}
    ad_weight = {}
    #query_group_id_query = {}
    campaignId_category  = {}
    campaignId_adId = {}

    with open(ad_input_file, "r") as lines:
        for line in lines:
            entry = json.loads(line.strip())
            ad_list.append(entry)
            adId_query[entry["adId"]] = entry["query"].lower()

    with open(query_ad_input_file) as json_data:
        query_camp_ad = json.load(json_data)
        #print query_camp_ad["1"]

    with open(campaign_weight_input_file) as json_data:
        campaign_weight = json.load(json_data)
        #print campaign_weight["8001"]

    with open(ad_weight_input_file) as json_data:
        ad_weight = json.load(json_data)
        #print ad_weight["1169"]

    #with open(query_group_id_query_file) as json_data:
    #    query_group_id_query = json.load(json_data)

    with open(campaignId_category_file) as json_data:
        campaignId_category = json.load(json_data)

    with open(campaignId_adId_file) as json_data:
        campaignId_adId = json.load(json_data)
    #test_weighted_sampling(query_camp_ad, campaign_weight)

    #split user to 4 level
    #level 0: 5% click for each query
    #level 1: 25% 1st 2 device id click for each query, rest 3 device_id click on 70% of query group, rest of 30% query  group no click
    #level 2: 30%  random  select 1 device_id click for 50% of query group
    #level 3: 40% never click

    num_ip = sum(1 for line in open(user_input_file))
    level_0_max_index = int(num_ip * 0.05)
    level_1_max_index = int(num_ip * 0.3)
    level_2_max_index = int(num_ip * 0.6)

    level_0_user = {}
    level_1_user = {}
    level_2_user = {}
    level_3_user = {}

    i = 0
    with open(user_input_file, "r") as lines:
        for line in lines:
            line = line.strip().strip("\n")
            fields = line.split(",")
            ip = fields[0]
            if i <= level_0_max_index:
                level_0_user[ip] = fields[1:6]
            if i > level_0_max_index and i <= level_1_max_index:
                level_1_user[ip] = fields[1:6]
            if i > level_1_max_index and i <= level_2_max_index:
                level_2_user[ip] = fields[1:6]
            if i > level_2_max_index:
                level_3_user[ip] = fields[1:6]
            i += 1

    click_log_output = open(click_log_output_file, "w")

    #negative sample (no click query)type

    #0: mismatched query_categr ads_category 20%
    #1: mismatched Query_CampaignId 10%
    #2: lowest campaignId weight, lowest adId weight 30%
    #3: matched but no click 40%
    num_ad_list = len(ad_list)
    negative_types = {}
    negative_types[0] = 0.2
    negative_types[1] = 0.1
    negative_types[2] = 0.3
    negative_types[3] = 0.4

    for ip in level_0_user:
        for device_id in level_0_user[ip]:
            for query_group_id in query_camp_ad:
                for i in range(1, 50):
                    fields = positive_sampling(ip, device_id, query_group_id, adId_query, query_camp_ad)
                    if valid(fields) == False :
                        continue
                    line = ",".join(fields)
                    click_log_output.write(line)
                    click_log_output.write('\n')

                for i in range(1, 10):
                    all_positive_Sampling(ip, device_id, query_group_id, adId_query, query_camp_ad,click_log_output)

    for ip in level_1_user:
        #positive sample
        for i in range(2):
            device_id = level_1_user[ip][i]
            for query_group_id in query_camp_ad:
                for i in range(1, 50):
                    fields = positive_sampling(ip, device_id, query_group_id, adId_query, query_camp_ad)
                    if valid(fields) == False :
                        continue
                    line = ",".join(fields)
                    click_log_output.write(line)
                    click_log_output.write('\n')

                for i in range(1, 10):
                    all_positive_Sampling(ip, device_id, query_group_id, adId_query, query_camp_ad,click_log_output)

        #negative
        for i in range(2,5):
            device_id = level_1_user[ip][i]
            query_group = {}
            query_group[1] = 0.7
            query_group[0] = 0.3
            for query_group_id in query_camp_ad:
                positive = weighted_sampling(query_group)
                if positive == 1:
                    for i in range(1, 50):
                        fields = positive_sampling(ip, device_id, query_group_id, adId_query, query_camp_ad)
                        if valid(fields) == False :
                            continue
                        line = ",".join(fields)
                        click_log_output.write(line)
                        click_log_output.write('\n')
                else:
                    for entry in ad_list:
                        if entry["query_group_id"] == query_group_id:
                            negative_type = weighted_sampling(negative_types)
                            fields = negative_sampling(ip, device_id, entry,query_camp_ad,campaign_weight,ad_weight,campaignId_category,campaignId_adId,negative_type)
                            if valid(fields) == False :
                                continue
                            line = ",".join(fields)
                            click_log_output.write(line)
                            click_log_output.write('\n')


    for ip in level_2_user:
        for query_group_id in query_camp_ad:
            millis = int(round(time.time() * 1000))
            random.seed(millis)
            r = random.randint(0,1)
            if r == 1:
                #positive sample
                for i in range(1, 10):
                    fields = positive_sampling(ip, level_2_user[ip][0], query_group_id, adId_query, query_camp_ad)
                    if valid(fields) == False :
                        continue
                    line = ",".join(fields)
                    click_log_output.write(line)
                    click_log_output.write('\n')

            else:
                #negative_type sample
                for entry in ad_list:
                    if entry["query_group_id"] == query_group_id:
                        negative_type = weighted_sampling(negative_types)
                        fields = negative_sampling(ip, level_2_user[ip][0], entry,query_camp_ad,campaign_weight,ad_weight,campaignId_category,campaignId_adId,negative_type)
                        if valid(fields) == False :
                            continue
                        line = ",".join(fields)
                        click_log_output.write(line)
                        click_log_output.write('\n')

            for i in range(1, 5):
                all_positive_Sampling(ip, device_id, query_group_id, adId_query, query_camp_ad,click_log_output)

            #negative
            for i in range(1,5):
                device_id = level_2_user[ip][i]
                for j in range(1, 4):
                    for entry in ad_list:
                            negative_type = weighted_sampling(negative_types)
                            fields = negative_sampling(ip, device_id, entry,query_camp_ad,campaign_weight,ad_weight,campaignId_category,campaignId_adId,negative_type)
                            if valid(fields) == False :
                                continue
                            line = ",".join(fields)
                            click_log_output.write(line)
                            click_log_output.write('\n')


    for ip in level_3_user:
        for device_id in level_3_user[ip]:
                for entry in ad_list:
                    negative_type = weighted_sampling(negative_types)
                    fields = negative_sampling(ip, device_id, entry,query_camp_ad,campaign_weight,ad_weight,campaignId_category,campaignId_adId,negative_type)
                    if valid(fields) == False :
                        continue
                    line = ",".join(fields)
                    click_log_output.write(line)
                    click_log_output.write('\n')


    click_log_output.close()
