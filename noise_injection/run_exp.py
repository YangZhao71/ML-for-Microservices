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
from joblib import Parallel, delayed

parser = argparse.ArgumentParser()
parser.add_argument('-q', dest='qps', type=int, required=True)
parser.add_argument('-i', dest='init', action="store_true")
parser.add_argument('-m', dest='machine', type=str, required=True)
parser.add_argument('-n', dest='num_iter', type=int, required=True)
parser.add_argument('-d', dest='duration', type=int, required=True)
parser.add_argument('--idx', dest='idx', type=int, default=0)
args = parser.parse_args()

qps = args.qps
machine = args.machine
num_iter = args.num_iter
init = args.init
duration = args.duration
idx = args.idx

# The folder to store the log files
log_dir = "/home/yg397/filers/" + machine + "/root_cause/logs/chain_10/noise_injection/"
# Application path 
app_dir = "/home/yg397/Research/root_cause/applications/chain_10/"
# The path of cpu_intensive.py
inter_dir = "/home/yg397/Research/root_cause/experiments/scripts/chain_10/"


if not os.path.exists(log_dir + "qps_" + str(qps)):
  os.mkdir(log_dir + "qps_" + str(qps))

def process_json(trace):
  df = json_normalize(trace["spans"])[["traceID","operationName", "startTime", "duration"]]
  return df

def write_csv(qps, i):
  json_file = log_dir + "qps_" + str(qps) + "/iter_" + str(i) + ".json"
  try:
    with open(json_file, 'r') as f:
      data = json.load(f)
    dfs = Parallel(n_jobs=20, backend="multiprocessing")(delayed(process_json)(trace) for trace in data["data"])
    df = pd.concat(dfs)

    csv_file = log_dir + "qps_" + str(qps) + "/iter_" + str(i) + ".csv"
    with open(csv_file, 'w') as f:
      df.to_csv(f)
    os.remove(json_file)
  except:
    print("Iteration", str(i), "failed")

n_services = 10

total_reqs = duration * qps
json_threads = []

for i in range(idx, num_iter):
  if (init and i == idx) or (i % 25 == 0 and i != 0):
    cmd = "cd " + app_dir + " &&\n"
    cmd += "docker-compose down &&\n"
    cmd += "docker system prune --volumes -f &&\n"
    cmd += "docker-compose up -d &&\n"
    cmd += "sleep 3 &&\n"
    # Pin CPU cores to each service
    for s in range(n_services):
      cmd += "docker update chain_10_service_" + \
          str(s) + "_1 --cpuset-cpus " + str(10 + s) + " &&\n"
    cmd = cmd[:-3]
    # print(cmd)
    subprocess.run(cmd, shell=True)

  interfere = np.random.binomial(1, 0.1, size=n_services)
  print(interfere)

  
  cores_list = []
  
  for s in range(n_services):
    cores_list.append([10 + s])

  # The data that will be passed to metrics.py
  pass_data = {}
  pass_data["cores_list"] = cores_list
  pass_data["interfere"] = interfere

  # print(pass_data)

  with open("./pass_data_" + machine + ".pkl", "wb") as f:
    pickle.dump(pass_data, f)

  start_ts = int(time.time() * 1000000)
  cmd = app_dir + "wrk2/wrk -D exp -t10 -c500 -d" + str(duration) + " -L http://" + machine + ":8100/api/service_0/rpc_0 -R " + \
        str(qps) + " &\n"

  # Lauanch interference jobs 
  for s in range(n_services):
    if interfere[s]:
      cmd += "taskset -c " + str(10 + s) + " python3 " + inter_dir + "cpu_intensive.py -i 3 -d " + str(duration) + " &\n"

  cmd += "/usr/bin/python3 ./read_metrics.py -q " + str(qps) + " -m \"" + machine + "\" -d " + str(duration) + " -i " + str(i) + " -l " + log_dir + " &&\n"
  cmd += "sleep 5"
  
  # print(cmd)
  subprocess.run(cmd, shell=True)
  end_ts = int(time.time() * 1000000) 

  cmd = "curl \"http://" + machine + ".ece.cornell.edu:16600/api/traces?service=nginx-web-server&limit=" + \
      str(total_reqs) + "&start=" + str(start_ts) + "&end=" + str(end_ts) + "\" > " + log_dir + "qps_" + str(qps)+ "/iter_" + str(i) + ".json"
  # print(cmd)
  subprocess.run(cmd, shell=True)


  t = threading.Thread(target=write_csv, args=(qps, i))
  json_threads.append(t)
  t.start()

for t in json_threads:
  t.join()

