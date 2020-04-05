import subprocess
import numpy as np
import os
import os.path
import argparse
import json
from pandas.io.json import json_normalize
import pandas as pd
import threading
import time
import random
import pickle

parser = argparse.ArgumentParser()
parser.add_argument('-q', dest='qps', type=int, required=True)
parser.add_argument('-i', dest='init', action="store_true")
parser.add_argument('-m', dest='machine', type=str, required=True)
parser.add_argument('-d', dest='duration', type=int, required=True)
args = parser.parse_args()

qps = args.qps
machine = args.machine

init = args.init
duration = args.duration


# log_dir = "/home/yg397/filers/" + machine + "/root_cause/logs/socialNetwork/compose-post/noise_injection/"
app_dir = "/home/yg397/Research/root_cause/applications/socialNetwork/"
inter_dir = "/home/yg397/Research/root_cause/experiments/scripts/socialNetwork/noise_injection/"

services = [
    "media-service",
    "user-service",
    "url-shorten-service",
    "user-timeline-service",
    "post-storage-service",
    "compose-post-service",
    "media-memcached",
    "user-mongodb",
    "post-storage-memcached",
    "user-memcached",
    "url-shorten-mongodb",
    "home-timeline-service",
    "user-mention-service",
    "text-service",
    "user-timeline-mongodb",
    "media-mongodb",
    "url-shorten-memcached",
    "unique-id-service",
    "user-timeline-redis",
    "post-storage-mongodb",
]

noise_intensity = {
    "media-service": [10, 100],
    "user-service": [10, 100],
    "url-shorten-service": [5, 40],
    "user-timeline-service": [5, 60],
    "post-storage-service": [5, 40],
    "compose-post-service": [5, 40],
    "media-memcached": [5, 40],
    "user-mongodb": [5, 40],
    "post-storage-memcached": [5, 40],
    "user-memcached": [10, 100],
    "url-shorten-mongodb": [10, 100],
    "home-timeline-service": [1, 100],
    "user-mention-service": [5, 100],
    "text-service": [4, 30],
    "user-timeline-mongodb": [4, 80],
    "media-mongodb": [1, 100],
    "url-shorten-memcached": [1, 100],
    "unique-id-service": [10, 100],
    "user-timeline-redis": [10, 100],
    "post-storage-mongodb": [10, 100],
}

num_cores = {
    "media-service": 1,
    "user-service": 1,
    "url-shorten-service": 1,
    "user-timeline-service": 1,
    "post-storage-service": 1,
    "compose-post-service": 4,
    "media-memcached": 1,
    "user-mongodb": 1,
    "post-storage-memcached": 1,
    "user-memcached": 1,
    "url-shorten-mongodb": 4,
    "home-timeline-service": 1,
    "user-mention-service": 1,
    "text-service": 4,
    "user-timeline-mongodb": 4,
    "media-mongodb": 1,
    "url-shorten-memcached": 1,
    "unique-id-service": 1,
    "user-timeline-redis": 1,
    "post-storage-mongodb": 4,
}


# if not os.path.exists(log_dir + "qps_" + str(qps)):
#   os.mkdir(log_dir + "qps_" + str(qps))

# def write_csv(qps, i):
#   json_file = log_dir + "qps_" + str(qps) + "/iter_" + str(i) + ".json"
#   try:
#     with open(json_file, 'r') as f:
#       data = json.load(f)
#       dfs = []
#       for trace in data["data"]:
#         dfs.append(json_normalize(trace["spans"])[["traceID", "operationName", "startTime", "duration"]])
#       df = pd.concat(dfs)
#     csv_file = log_dir + "qps_" + str(qps) + "/iter_" + str(i) + ".csv"
#     with open(csv_file, 'w') as f:
#       df.to_csv(f)
#     os.remove(json_file)
#   except :
#     print("Iteration", str(i), "failed")

n_services = len(services)

total_reqs = duration * qps
json_threads = []


if init:
  cmd = "cd " + app_dir + " &&\n"
  cmd += "docker-compose down &&\n"
  cmd += "docker system prune --volumes -f &&\n"
  cmd += "docker-compose up -d &&\n"
  cmd += "sleep 10 &&\n"
  cmd += "python3.5 ./scripts/init_social_graph.py --ip_addr=" + machine + " --port=8090 " \
  "--data_path=./datasets/social-graph/socfb-Reed98/socfb-Reed98.mtx &&\n"
  cmd += "wrk2/wrk -D exp -t10 -c500 -d" + str(duration) + \
    " -s wrk2/scripts/social-network/compose-post.lua -L http://" + machine + \
    ":8090/wrk2-api/post/compose -R " + str(qps) + " &&\n"
  cmd += "sleep 10 \n"
  # print(cmd)
  subprocess.run(cmd, shell=True)

# if_scale_out = np.random.randint(low=0, high=2, size=10)
interfere = np.random.binomial(1, 0.0, size=n_services)
interfere[services.index("user-timeline-mongodb")] = 1
print(interfere)
# interfere_service = services[np.random.randint(low=0, high=n_services)]
# if_interfere = 1
# interfere_service = services[19]
# print(if_interfere, interfere_service)


cmd = ""
cores_list = []


aval_core_even = 10
aval_core_odd = 11

for s in services:
  cores = [str(min(aval_core_even, aval_core_odd) + 2 * i) for i in range(num_cores[s])]
  if aval_core_even < aval_core_odd:
    aval_core_even += 2 * num_cores[s]
  else:
    aval_core_odd += 2 * num_cores[s]

  cores_str = ",".join(cores)

  cmd += "docker update socialnetwork_" + \
      s + "_1 --cpuset-cpus " + cores_str + " &&\n"
  cores_list.append(cores)
cmd = cmd[:-3]
subprocess.run(cmd, shell=True)

pass_data = {}
pass_data["cores_list"] = cores_list
pass_data["interfere"] = interfere
# pass_data["interfere_service"] = interfere_service


with open("./pass_data_" + machine + ".pkl", "wb") as f:
  pickle.dump(pass_data, f)

start_ts = int(time.time() * 1000000)
cmd = app_dir + "wrk2/wrk -D exp -t10 -c500 -d" + str(duration) + \
    " -s " + app_dir + "wrk2/scripts/social-network/compose-post.lua -L http://" + machine + \
    ":8090/wrk2-api/post/compose -R " + str(qps) + " &\n"
for s in services:
  if interfere[services.index(s)] == 1:
    for j in range(noise_intensity[s][0]):
      cmd += "taskset -c " + ",".join(cores_list[services.index(s)]) + " python3 " + inter_dir + "cpu_intensive.py -i " + str(noise_intensity[s][1]) + " -d " + str(duration) + " &\n"
cmd = cmd[:-2]
# cmd += "/usr/bin/python3.5 ./read_metrics.py -q " + str(qps) + " -m \"" + machine + "\" -d " + str(duration) + " -i " + str(i) + " -l " + log_dir + " &&\n"
# cmd += "sleep 5"

# print(cmd)
# subprocess.run(cmd, shell=True)
end_ts = int(time.time() * 1000000) 

# cmd = "curl \"http://" + machine + ".ece.cornell.edu:16686/api/traces?service=nginx-web-server&limit=" + \
#     str(total_reqs) + "&start=" + str(start_ts) + "&end=" + str(end_ts) + "\" > " + log_dir + "qps_" + str(qps)+ "/iter_" + str(i) + ".json"
# print(cmd)

