#!/usr/bin/env python
# -*- coding: utf8 -*-

from subprocess import call, check_output
import shlex
import os
import re
import time
import logging


# benchmark shell scripts
CONFIG_PREFIX = 'config.%s.sh'
REPLACE_PREFIX = 'sed -i "s/%s_[0-9a-zA-Z]*$/%s_%s/g" %s'
BUILD_PREFIX = './build.%s.sh'
RUN_PREFIX = './run.%s.%s.sh'
DROP_CACHE_CMD = 'sudo ./drop.cache.sh'
# hamr config
HAMR_CONFIG = '/home/hamr/usr/hamr-%s/conf/hamr-site.xml'
CLUSTER = ["master", "slave01", "slave02", "slave03"]
# wait between dataset executing
WAITING = 30
# log
LOG_FILE = 'benchmark.log'
LOG_FARMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# create a file handler
handler = logging.FileHandler(LOG_FILE)
handler.setLevel(logging.INFO)
# create a logging format
formatter = logging.Formatter(LOG_FARMAT)
handler.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(handler)

def run(benchmarks, rounds=1):
    for name in benchmarks.keys():
        if benchmarks[name]["active"] == "yes":
            logger.info("Benchmark %s is active" % (name, ))
            for value in benchmarks[name]["dataset"]:
                # Drop cache at first
                cmd_output = drop_cache()
                logger.debug(cmd_output)

                # Run with each dataset
                dataset = "[%s_%s]" % (name, value)
                logger.info("%s will begin after %s sec ..." % (dataset, WAITING))
                time.sleep(WAITING)

                # generate config
                config_cmd = CONFIG_PREFIX % (name.lower())
                replace_dataset_cmd = REPLACE_PREFIX % (
                    name, name, value, config_cmd)
                args = shlex.split(replace_dataset_cmd)
                logger.debug(replace_dataset_cmd)
                try:
                    call(args, cwd=cwd)
                    # checkout replaced or not
                    cmd_output = check_output(["grep", "DATASET_NAME", 
                        config_cmd], cwd=cwd)
                    logger.debug(cmd_output)
                except Exception, e:
                    logger.debug("%s config failed: %s" % (dataset, e))

                # build
                build_cmd = BUILD_PREFIX % (name.lower())
                logger.debug(build_cmd)
                try:
                    cmd_output = check_output(build_cmd, cwd=cwd)
                except Exception, e:
                    logger.debug("%s build failed: %s" % (dataset, e))
                
                # run
                run_cmd = RUN_PREFIX % (name.lower(), file_system)
                logger.debug(run_cmd)
                total_execution_time = 0
                succeeded_rounds = 0
                
                for i in range(rounds):
                    try:
                        cmd_output = check_output(run_cmd, cwd=cwd)
                        succeeded_rounds += 1
                        logger.debug(cmd_output)
                        execution_time = extract_time(cmd_output)
                        total_execution_time += execution_time
                        logger.info("%s round [%s/%s] done, execution %s ms" % 
                            (dataset, i+1, rounds, execution_time))
                    except Exception, e:
                        logger.info("%s round [%s/%s] failed: %s" % 
                            (dataset, i+1, rounds, e))
                    finally:
                        cmd_ouput = drop_cache()
                        logger.debug(cmd_ouput)
                if succeeded_rounds is 0:
                    logger.info("%s done, all attempts failed." % 
                        (dataset,))
                else:
                    avg_execution_time = total_execution_time/succeeded_rounds
                    logger.info("%s done, avg execution %s ms" % 
                        (dataset, avg_execution_time))               
        else:
            logger.info("Benchmark %s is not active" % (name, ))

def drop_cache():
    args = shlex.split(DROP_CACHE_CMD)
    return check_output(args)

def setup(ver="0.4.1", conn="rmq"):
    logger.info("setup HAMR %s with %s" % (ver, conn))
    hamr_config = HAMR_CONFIG % (ver, )
    for node in CLUSTER:
        # copy new config to each node
        setup_cmd = 'scp %s.%s %s:%s' % (
            hamr_config, conn, node, hamr_config)
        args = shlex.split(setup_cmd)
        logger.debug(setup_cmd)
        try:
            cmd_output = check_output(args, cwd=cwd)
            logger.debug(cmd_output)
        except Exception, e:
            logger.debug("setup failed")
            raise e

def stop_spark(ver="1.5.1"):
    logger.info("stop Spark %s" % (ver, ))
    for node in CLUSTER:
        # copy new config to each node
        stop_cmd = 'ssh %s usr/spark-%s-bin-hadoop2.4/sbin/stop-all.sh' % (node, ver)
        args = shlex.split(stop_cmd)
        logger.debug(stop_cmd)
        try:
            cmd_output = check_output(args, cwd=cwd)
            logger.debug(cmd_output)
        except Exception, e:
            logger.debug("stop Spark failed")
            raise e

def extract_time(search_text):
    pattern = re.compile(r"(\d*) ms")
    match = pattern.search(search_text)
    if match:
        return float(match.group(1))
    else:
        return 0.0

if __name__ == "__main__":
    config_file = "config.allinone.json"
    config_data = open(config_file).read()
    import json
    config = json.loads(config_data)
    file_system = config["FS"]
    conn = config["connection"]
    ver = config["version"]
    benchmarks = config["benchmarks"]
    logger.debug(benchmarks)
    cwd = os.getcwd()
    setup(ver, conn)
    run(benchmarks)
#    search_text = "applicable\nexecution time:  54544 ms\n"
#    extract_time(search_text)