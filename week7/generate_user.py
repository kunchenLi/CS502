import os
import sys
import json
import random
from sets import Set
import time

if __name__ == "__main__":
    user_output_file = sys.argv[1]
    user_output = open(user_output_file, "w")
    num_ip = 10000
    ip_set = Set()
    device_id_set = Set()
    while len(ip_set) < num_ip:
        if len(ip_set) % 100 == 0:
            print "generated number of ip:" + str(len(ip_set))
        millis = int(round(time.time() * 1000))
        random.seed(millis)
        ip = random.randint(10000,100000)
        if ip not in ip_set:
            ip_set.add(ip)

    for ip in ip_set:
        if len(device_id_set) % 1000 == 0:
            print "generated number of device_id:" + str(len(device_id_set))
        i = 0
        user = []
        user.append(str(ip))
        while i < 5:
            millis = int(round(time.time() * 1000))
            random.seed(millis)
            device_id = random.randint(1,100000)
            if device_id not in device_id_set:
                device_id_set.add(device_id)
                user.append(str(device_id))
                i+=1
        line = ",".join(user)
        user_output.write(line)
        user_output.write('\n')

    user_output.close()
