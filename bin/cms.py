#!/bin/env python
# -*- coding: utf8 -*-

from util import mem_stat

CLUSTER = ["master", "slave01", "slave02", "slave03"]

def run():
    for node in CLUSTER:
        print(mem_stat(node))

if __name__ == "__main__":
    run()
