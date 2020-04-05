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
parser.add_argument('-i', dest='init', action="store_true")
parser.add_argument('-m', dest='machine', type=str, required=True)
args = parser.parse_args()

machine = args.machine
init = args.init

data_dir = "/home/yg397/Research/root_cause/experiments/data/socialNetwork/"
app_dir = "/home/yg397/Research/root_cause/applications/socialNetwork/"

duration = 10
qps = 100

total_reqs = duration * qps
json_threads = []


if init:
  cmd = "cd " + app_dir + " &&\n"
  cmd += "docker-compose down &&\n"
  cmd += "docker system prune --volumes -f &&\n"
  cmd += "docker-compose up -d &&\n"
  cmd += "sleep 10 &&\n"
  cmd += "python3.5 ./scripts/init_social_graph.py " \
  "./datasets/social-graph/socfb-Reed98/socfb-Reed98.mtx"
  subprocess.run(cmd, shell=True)




start_ts = int(time.time() * 1000000)
cmd = app_dir + "wrk2/wrk -D exp -t10 -c500 -d" + str(duration) + \
    " -s " + app_dir + "wrk2/scripts/social-network/compose-post.lua http://" + machine + \
    ":8090/wrk2-api/post/compose -R " + str(qps) + " \n"
cmd += "sleep 5"

# print(cmd)
subprocess.run(cmd, shell=True)
end_ts = int(time.time() * 1000000) 

cmd = "curl \"http://" + machine + ".ece.cornell.edu:16686/api/traces?service=nginx-web-server&limit=" + \
    str(total_reqs) + "&start=" + str(start_ts) + "&end=" + str(end_ts) + "\" > " + data_dir + "tmp_jaeger_traces.json"
# print(cmd)
subprocess.run(cmd, shell=True)

with open(data_dir + "tmp_jaeger_traces.json", "r") as f:
  data = json.load(f)
  trace = data['data'][0]
  spans = trace['spans']
  processes = trace['processes']

  spans_dict = {}


  for span in spans:
    span_dict = {}
    span_dict["span_name"] = span['operationName']
    if len(span['references']) > 0:
      span_dict["parent_span_id"] = span['references'][0]['spanID']
    span_dict["service_name"] = processes[span['processID']]['serviceName']
    spans_dict[span['spanID']] = span_dict
  
  for span_id, span in spans_dict.items():
    if 'parent_span_id' in span:
      span['parent_span_name'] = spans_dict[span['parent_span_id']]['span_name']
  
  with open(data_dir + 'rpc_deps.csv', 'w') as f:
    f.write('parent,child\n')
    for span_id, span in spans_dict.items():
      f.write(span['service_name'] + ',' + span['span_name'] + '\n')
      if 'parent_span_id' in span:
        f.write(span['span_name'] + ',' + span['parent_span_name'] + '\n')

