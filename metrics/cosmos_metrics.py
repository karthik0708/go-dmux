import os
import re
import time
from datetime import timedelta

import requests
from timeloop import Timeloop

endpoint = "http://localhost:9999/metrics"
tl = Timeloop()
TIME_BETWEEN_POLLING = 3
metric_prefix = "dmux"


def push_metric(metric, current_epoch):
  result = re.findall(r"(\w+)({(\S+)})? (\S+)", metric)
  result = result[0]
  metric_name = result[0]
  metric_tags = result[2]
  metric_value = result[3]

  metric_tags = metric_tags.replace(",", " ")
  metric_tags = metric_tags.replace('"', '')

  tsdbMetric = current_epoch + " " + metric_prefix + "." + metric_name + " " + metric_value + " " + metric_tags

  # print(tsdbMetric)

  os.system("echo "+metric_tags+" | /usr/bin/cosmos")


def process(metrics):
  metrics = metrics.splitlines(True)
  metrics = [x for x in metrics if not x.startswith('#')]
  metrics = [x for x in metrics if not x.startswith("go_")]
  for metric in metrics:
    push_metric(metric, str(int(time.time())))
  print("++++++++++++++++++++++++++++++++++++++++++++++++++")


@tl.job(interval=timedelta(seconds=TIME_BETWEEN_POLLING))
def hit_request():
  try:
    response = requests.get(endpoint)
    process(response.text)
  except:
    print("error connecting to end point")
    return


if __name__ == "__main__":
  tl.start(block=True)
