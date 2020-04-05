import json

import sys
from joblib import Parallel, delayed
from itertools import product
import argparse
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import os


parser = argparse.ArgumentParser()
parser.add_argument('-q', dest='qps', type=int, required=True)
parser.add_argument('--train', dest='train_size', type=int, required=True)
parser.add_argument('--test', dest='test_size', type=int, required=True)
parser.add_argument('--eval', dest='eval_size', type=int, required=True)
args = parser.parse_args()

qps = args.qps
train_size = args.train_size
test_size = args.test_size
eval_size = args.eval_size

log_dir = "/home/yg397/filers/ath-9/root_cause/logs/chain_10/noise_injection/qps_500/"

output_dir = "/home/yg397/Research/root_cause/experiments/data/chain_10/"

span_names = [
    "rpc_0_client",
    "rpc_0_server",
    "rpc_1_client",
    "rpc_1_server",
    "rpc_2_client",
    "rpc_2_server",
    "rpc_3_client",
    "rpc_3_server",
    "rpc_4_client",
    "rpc_4_server",
    "rpc_5_client",
    "rpc_5_server",
    "rpc_6_client",
    "rpc_6_server",
    "rpc_7_client",
    "rpc_7_server",
    "rpc_8_client",
    "rpc_8_server",
    "rpc_9_client",
    "rpc_9_server", 
]

n_spans = 22

def gen_df(i, qps):
  filename = log_dir + "iter_" + str(i) + ".csv"
  try:
    with open(filename, 'r') as f:
      df = pd.read_csv(f)

      # Filter out traces with incomplete spans
      count_df = df[['traceID', 'startTime']].groupby(['traceID'], as_index=False).count()
      count_df.columns = ['traceID', 'count']
      count_df = count_df[count_df['count'] == 22]
      df = df.merge(count_df, on='traceID', how='inner')[['traceID', 'operationName', 'duration', 'startTime']]

      duration_df = pd.pivot_table(df[['traceID', 'operationName', 'duration']], index='traceID', columns='operationName', values='duration')[span_names]

      min_df = df[['traceID', 'startTime']].groupby(['traceID'], as_index=False).min()
      min_df.columns = ['traceID', 'minStartTime']
      min_df['minStartTime'] = pd.to_datetime(min_df['minStartTime'], unit='us')

      df = duration_df.merge(min_df, on='traceID', how='inner')

      dfs = []
      quantiles = [q for q in [95, 99]]
      for quantile in quantiles:
        tmp_df = df.groupby(pd.Grouper(key='minStartTime', freq='3600S')).quantile(quantile / 100).copy()
        rename_cols = {}
        for col in tmp_df.columns:
          rename_cols[col] = col + "_" + str(quantile) + "th"
        tmp_df.rename(columns=rename_cols, inplace=True)
        dfs.append(tmp_df)

      result_df = dfs[0]
      
      for tmp_df in dfs[1:]:
        result_df = result_df.merge(tmp_df, on="minStartTime")
      for name in span_names:
        cols = [name + "_" + str(q) + "th" for q in quantiles]
        result_df[name] = result_df[cols].values.astype("int32").tolist()
      result_df = result_df[span_names]
      
      result_df["qps"] = qps    
    
    metric_filename = log_dir + "metrics_" + str(i) + ".csv"

    with open(metric_filename, 'r') as f:
      metrics_df = pd.read_csv(f)
      metrics_df.set_index('name', inplace=True)
      metrics_df_out = metrics_df.stack()
      metrics_df_out.index = metrics_df_out.index.map('{0[0]}_{0[1]}'.format)
      metrics_df_out = metrics_df_out.to_frame().T

      re_metrics_df_out = pd.DataFrame(np.repeat(metrics_df_out.values,result_df.shape[0],axis=0))
      re_metrics_df_out.columns =metrics_df_out.columns
    
    # concat with index, make index as a column
    result_df = result_df.reset_index().reset_index()
    re_metrics_df_out = re_metrics_df_out.reset_index()
    result_df = pd.concat((result_df, re_metrics_df_out), axis=1)
    result_df.drop(columns=['index', 'minStartTime'], axis=1, inplace=True)

    print("QPS =", qps, "Iteration", str(i), "finished")
    return result_df
  except:
    print("Error", i)
    exit

gen_df(0, 500)

total_size = train_size + test_size + eval_size
output_dir += "train_" + str(train_size) + "/"
try:
  os.mkdir(output_dir)
except:
  pass


# gen_df(0, 500)

dfs = Parallel(n_jobs=100, backend="multiprocessing")(delayed(gen_df) (i, qps) for i in range(total_size))
df = pd.concat(dfs, ignore_index=True, sort=False)
if not test_size and not eval_size:
  with open(output_dir + "latency-noise_inject-train.csv", "w") as f:
    df.to_csv(f)
elif test_size and not eval_size:
  test_size = len(df.index) - train_size
  X_train, X_test = train_test_split(df, train_size=train_size, test_size=test_size, shuffle=True)
  df_train = pd.DataFrame(data=X_train, columns=df.columns)
  df_test = pd.DataFrame(data=X_test, columns=df.columns)
  with open(output_dir + "latency-noise_inject-train.csv", "w") as f:
    df_train.to_csv(f)
  with open(output_dir + "latency-noise_inject-test.csv", "w") as f:
    df_test.to_csv(f)
elif test_size and eval_size:
  eval_size = len(df.index) - train_size - test_size
  X_train, X_test_eval = train_test_split(df, train_size=train_size, test_size=test_size + eval_size, shuffle=True)
  X_test, X_eval = train_test_split(X_test_eval, train_size=test_size, test_size=eval_size, shuffle=True)
  df_train = pd.DataFrame(data=X_train, columns=df.columns)
  df_test = pd.DataFrame(data=X_test, columns=df.columns)
  df_eval = pd.DataFrame(data=X_eval, columns=df.columns)
  with open(output_dir + "latency-noise_inject-train.csv", "w") as f:
    df_train.to_csv(f)
  with open(output_dir + "latency-noise_inject-test.csv", "w") as f:
    df_test.to_csv(f)
  with open(output_dir + "latency-noise_inject-eval.csv", "w") as f:
    df_eval.to_csv(f)