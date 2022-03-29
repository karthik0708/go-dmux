import json
import os
import re

import requests
import time

endpoint = "http://localhost:9999/metrics"
TIME_BETWEEN_POLLING = 5
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

    os.system("echo " + tsdbMetric + " | /usr/bin/cosmos")


def process(metrics, req_type):
    metrics = metrics.splitlines(True)

    for metric in metrics:
        if metric.startswith('#'):
            continue

        if req_type == "Custom" and metric.startswith("go_"):
            continue

        if req_type == "System" and (not metric.startswith("go_")):
            continue

        push_metric(metric, str(int(time.time())))
    print("++++++++++++++++++++++++++++++++++++++++++++++++++")


def hit_request(req_type):
    try:
        response = requests.get(endpoint)
    except requests.ConnectionError:
        print("error connecting to end point")
        return

    try:
        process(response.text, req_type)
    except:
        print("error in processing")
        return


if __name__ == "__main__":
    config = open(os.path.join(os.getcwd(), '..', 'conf.json'))
    config = json.load(config)

    while True:
        hit_request(config['metric_req'])
        time.sleep(TIME_BETWEEN_POLLING)
