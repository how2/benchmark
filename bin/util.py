#!/bin/env python
# -*- coding: utf8 -*-

from subprocess import check_output
import shlex

def rmq_status(node):
    cmd = "ssh %s rabbitmqctl cluster_status" % (node, )
    return execution_result(cmd)

def mem_stat(node):
    cmd = "ssh %s free -g" % (node, )
    return execution_result(cmd)

def execution_result(stop_cmd):
    args = shlex.split(stop_cmd)
    try:
        cmd_output = check_output(args)
    except Exception, e:
        cmd_output = e
    return cmd_output

def extract_time(search_text):
    pattern = re.compile(r"(\d*) ms")
    match = pattern.search(search_text)
    if match:
        return float(match.group(1))
    else:
        return 0.0

if __name__ == "__main__":
    rmq_status()

