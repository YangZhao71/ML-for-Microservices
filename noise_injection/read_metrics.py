import subprocess
import argparse
import pandas as pd
import time
import pickle
import numpy as np
from concurrent.futures import ThreadPoolExecutor

parser = argparse.ArgumentParser()
parser.add_argument('-q', dest='qps', type=int, required=True)
parser.add_argument('-d', dest='duration', type=int, required=True)
parser.add_argument('-m', dest='machine', type=str, required=True)
parser.add_argument('-i', dest='iteration', type=str, required=True)
parser.add_argument('-l', dest='log_dir', type=str, required=True)
args = parser.parse_args()

qps = args.qps
duration = args.duration
machine = args.machine
iteration = args.iteration
log_dir = args.log_dir

container_prefix = "chain_10"

with open("./pass_data_" + machine + ".pkl", "rb") as f:
  pass_data = pickle.load(f)

cores_list = pass_data["cores_list"]
interfere = pass_data["interfere"]

# number of services
n_services = 10

def remove_prefix(text, prefix):
  if text.startswith(prefix):
    return text[len(prefix):]
  return text

def extract_name(raw_name):
  prefix = container_prefix + "_"
  raw_name = raw_name[:-2]
  return remove_prefix(raw_name, prefix)

def extract_cpu_util(raw_cpu_util):
  return float(raw_cpu_util[:-1])

def extract_mem_util(raw_mem_util):
  return float(raw_mem_util[:-1])

def extract_netio(raw_netio):
  rd, wr = raw_netio.split(" / ")
  rd_kb = 0
  wr_kb = 0
  if rd.endswith("kB"):
    rd_kb = float(rd[:-2])
  elif rd.endswith("MB"):
    rd_kb = float(rd[:-2]) * 2**10
  elif rd.endswith("GB"):
    rd_kb = float(rd[:-2]) * 2**20
  elif rd.endswith("B"):
    rd_kb = float(rd[:-1]) / 2**10
  if wr.endswith("kB"):
    wr_kb = float(wr[:-2])
  elif wr.endswith("MB"):
    wr_kb = float(wr[:-2]) * 2**10
  elif wr.endswith("GB"):
    wr_kb = float(wr[:-2]) * 2**20
  elif wr.endswith("B"):
    wr_kb = float(wr[:-1]) / 2**10
  return rd_kb, wr_kb

def extract_blkio(raw_blkio):
  raw_blkio = raw_blkio[:-1]
  rd, wr = raw_blkio.split(" / ")
  rd_kb = 0
  wr_kb = 0
  if rd.endswith("kB"):
    rd_kb = float(rd[:-2])
  elif rd.endswith("MB"):
    rd_kb = float(rd[:-2]) * 2**10
  elif rd.endswith("GB"):
    rd_kb = float(rd[:-2]) * 2**20
  elif rd.endswith("B"):
    rd_kb = float(rd[:-1]) / 2**10
  if wr.endswith("kB"):
    wr_kb = float(wr[:-2])
  elif wr.endswith("MB"):
    wr_kb = float(wr[:-2]) * 2**10
  elif wr.endswith("GB"):
    wr_kb = float(wr[:-2]) * 2**20
  elif wr.endswith("B"):
    wr_kb = float(wr[:-1]) / 2**10
  return rd_kb, wr_kb

def get_ip_addrs():
  ip_addrs = {}
  for s in n_services:
    cmd = "docker inspect -f \'{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}\' " + container_prefix + "_service_" + str(s) + "_1"
    ip_addrs[s] = str(subprocess.check_output(cmd, shell=True))
  return ip_addrs

def docker_stat_worker():
  cmd = "docker stats --no-stream --format \"table {{.Name}} : {{.CPUPerc}} : {{.MemPerc}} : {{.NetIO}} : {{.BlockIO}}\""
  docker_out = subprocess.check_output(cmd, shell=True).decode("utf-8")
  return docker_out


def mpstat_worker():
  cmd = "mpstat -P ALL 1 1 | tail -n +5 | awk \'{print (100-$13)\'}"
  mpstat_out = subprocess.check_output(cmd, shell=True).decode("utf-8")
  return mpstat_out

def ping_worker(s):
  cmd = "ping $(docker inspect -f \'{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}\' " + container_prefix + "_service_" + str(s) + "_1) -c 1 | grep rtt"
  success = 0
  while not success:
    try:
      ping_out = subprocess.check_output(cmd, shell=True).decode("utf-8")
      success = 1
    except:
      pass
  ping_out = ping_out.split("=")[1].split("/")[1]
  return float(ping_out)

def netstat_worker(s):
  cmd = "docker exec " + container_prefix + "_service_" + str(s) + "_1 netstat -s | grep segments"
  netstat_out = subprocess.check_output(cmd, shell=True).decode("utf-8")
  lines = netstat_out.split('\n')
  for line in lines:
    if line.endswith("send out"):
      segments_sent = float(line.split()[0])
    elif line.endswith("retransmited"):
      segments_retransmit = float(line.split()[0])
  return segments_sent, segments_retransmit


def read_stats():
  start_timestamp = int(round(time.time()))
  metrics_df = pd.DataFrame(columns=["interfere", "name", "cpu_util", "core_util", "num_cores", "mem_util", "netio_rd", "netio_wr", "blkio_rd", "blkio_wr", "timestamp", "ping", "pkt_loss_rate"])

  while int(round(time.time())) - start_timestamp <= duration - 3:
    with ThreadPoolExecutor(max_workers=128) as executor:
      docker_stat_future = executor.submit(docker_stat_worker)
      mpstat_future = executor.submit(mpstat_worker)
      ping_futures = []
      netstat_futures = []
      for s in range(n_services):
        ping_futures.append(executor.submit(ping_worker, s))
        netstat_futures.append(executor.submit(netstat_worker, s))
      
      docker_stat_out = docker_stat_future.result()
      mpstat_out = mpstat_future.result()
      ping_outs = []
      netstat_outs = []
      for s in range(n_services):
        ping_outs.append(ping_futures[s].result())
        netstat_outs.append(netstat_futures[s].result())

    mpstat_list = mpstat_out.split('\n')
    timestamp = int(round(time.time()))

    for line in docker_stat_out.split("\n"):
      if line.startswith(container_prefix):
        fields = line.split(" : ")
        
        # Extract metrics from docker stats
        name = extract_name(fields[0])
        if not name.startswith("service"):
          continue
        cpu_util = extract_cpu_util(fields[1])
        mem_util = extract_mem_util(fields[2])
        netio_rd, netio_wr = extract_netio(fields[3])
        blkio_rd, blkio_wr = extract_blkio(fields[4])

        # Extract metrics from mpstat
        s = int(name.split('_')[-1])
        n_cores = float(len(cores_list[s]))
        core_util = 0
        count = 0
        for c in cores_list[s]:
          count += 1.0
          core_util += float(mpstat_list[c])
        core_util /= count

        # Extract metrics from ping
        ping = ping_outs[s]
        segments_sent, segments_retransmit = netstat_outs[s]
        
        if_interfere = float(interfere[s])

        data_dict = {
          "name": name, 
          "cpu_util": cpu_util, 
          "core_util": core_util,
          "num_cores": n_cores,
          "interfere": if_interfere,
          "mem_util": mem_util, 
          "netio_rd": netio_rd, 
          "netio_wr": netio_wr, 
          "blkio_rd": blkio_rd, 
          "blkio_wr": blkio_wr, 
          "ping": ping,
          "segments_sent": segments_sent,
          "segments_retransmit": segments_retransmit,
          "timestamp": timestamp, 
        }


        metrics_df = metrics_df.append(data_dict, ignore_index=True)

  metrics_df = metrics_df.groupby(["name"], as_index=False).agg(
    {
      "timestamp": np.ptp,
      "cpu_util": np.mean, 
      "core_util": np.mean, 
      "mem_util": np.mean, 
      "num_cores": np.mean, 
      "interfere": np.mean,
      "netio_rd": np.ptp, 
      "netio_wr": np.ptp, 
      "blkio_rd": np.ptp, 
      "blkio_wr": np.ptp, 
      "ping": np.mean,
      "segments_sent": np.ptp, 
      "segments_retransmit": np.ptp
    }
  )

  metrics_df["netio_rd"] = metrics_df["netio_rd"] / metrics_df["timestamp"]
  metrics_df["netio_wr"] = metrics_df["netio_wr"] / metrics_df["timestamp"]
  metrics_df["blkio_rd"] = metrics_df["blkio_rd"] / metrics_df["timestamp"]
  metrics_df["blkio_wr"] = metrics_df["blkio_wr"] / metrics_df["timestamp"]
  metrics_df["blkio_wr"] = metrics_df["blkio_wr"] / metrics_df["timestamp"]

  metrics_df["pkt_loss_rate"] = (metrics_df["segments_retransmit"] / metrics_df["segments_sent"]).replace(np.inf, 0).replace(np.nan, 0)

  metrics_df = metrics_df[["interfere", "num_cores", "cpu_util", "core_util", "mem_util", "netio_rd", "netio_wr", "blkio_rd", "blkio_wr", "ping", "pkt_loss_rate"]]
  with open(log_dir + "qps_" + str(qps) + "/metrics_" + str(iteration) + ".csv", "w") as f:
    metrics_df.to_csv(f)

read_stats()