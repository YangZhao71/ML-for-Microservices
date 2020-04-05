
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
parser.add_argument('-m', dest='machine', type=str, required=True)
args = parser.parse_args()

qps = args.qps
machine = args.machine

log_dir = "/home/yg397/filers/" + machine + "/root_cause/logs/chain_10/noise_injection/"
app_dir = "/home/yg397/Research/root_cause/applications/chain_10/"
inter_dir = "/home/yg397/Research/root_cause/experiments/scripts/chain_10/noise_injection/"


if not os.path.exists(log_dir + "qps_" + str(qps)):
  os.mkdir(log_dir + "qps_" + str(qps))


def process_json(trace):
  df = json_normalize(trace["spans"])[["traceID","operationName", "startTime", "duration"]]
  return df

def write_csv(qps, i):
  json_file = log_dir + "qps_" + str(qps) + "/iter_" + str(i) + ".json"
  # try:
  with open(json_file, 'r') as f:
    data = json.load(f)
  dfs = Parallel(n_jobs=20, backend="multiprocessing")(delayed(process_json)(trace) for trace in data["data"])
  df = pd.concat(dfs)

  csv_file = log_dir + "qps_" + str(qps) + "/iter_" + str(i) + ".csv"
  with open(csv_file, 'w') as f:
    df.to_csv(f)
  os.remove(json_file)
  # except:
  #   print("Iteration", str(i), "failed")

write_csv(500, 4125)

# for i in range(4100, 4200):
#   write_csv(500, i)