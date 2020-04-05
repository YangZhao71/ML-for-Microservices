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
import logging
import docker

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

stack_name = "yg397_chain_10"
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# The folder to store the log files
log_dir = "/home/yg397/filers/" + machine + "/root_cause/logs/chain_10/noise_injection/"
# Application path 
app_dir = "/home/yg397/Research/root_cause/applications/chain_10/"
# The path of cpu_intensive.py
inter_dir = "/home/yg397/Research/root_cause/experiments/scripts/chain_10/"

client = docker.DockerClient(base_url='unix://var/run/docker.sock')

def docker_stack_deploy(client, machine):
  cmd = "cd " + app_dir + " &&\n"
  cmd += "docker stack rm " + stack_name
  subprocess.run(cmd, shell=True)
  
  logging.info('Wait for services to stop')
  removed = False 
  while removed is not True: 
    cmd = "docker ps | grep " + stack_name
    try:
      out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
      time.sleep(1)
    except:
      removed = True
  logging.info('Docker stack rm finished')

  logging.info('Docker stack deploy start')
  cmd = "cd " + app_dir + " &&\n"
  cmd += "docker system prune --volumes -f &&\n"
  cmd += "docker stack deploy --compose-file docker-compose-swarm.yml " + stack_name
  subprocess.run(cmd, shell=True)

  # Wait for docker stack services to converge
  logging.info('Docker stack deploy finished, waiting for services to converge')
  converged = False
  while converged is not True:
    for service in client.services.list():
      cmd = 'docker service ls --format \'{{.Replicas}}\' --filter \'id=' + service.id + '\''
      out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True).strip()
      actual = int(out.split('/')[0])
      desired = int(out.split('/')[1])
      converged = actual == desired
      if not converged:
        break
    time.sleep(1)
  logging.info('services converged')

docker_stack_deploy(client, machine)
# print([s.name for s in client.services.list() if s.name.startswith(stack_name)])

