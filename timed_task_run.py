#!/usr/bin/env python
# -*- coding:utf-8 -*-
from subprocess import *
from timed_task_config import CONFIG_JSON
from datetime import datetime
import time
import os

CURRENT_DIR = os.path.split(os.path.abspath(__file__))[0]
RUN_CMD = 'python ' + os.path.join(CURRENT_DIR, 'run.py')
CONFIG_DAT_PATH = os.path.join(CURRENT_DIR, 'config.dat')

task = None


def do_task():
    global task
    task = None
    task = Popen(RUN_CMD, shell=True, stdout=PIPE, stdin=PIPE, stderr=PIPE)
    out, err = task.communicate()
    task = None
    if err:
        print 'Error: ' + err
    else:
        print "Executed '" + RUN_CMD + "'"
        print out


def change_config(local_path, bucket, remote_dir):
    j = {
        'Operation': 'upload',
        'IgnoreExist': 'false',
        'LocalPath': local_path,
        'BucketNameFixed': bucket,
        'RemoteDir': remote_dir
    }

    new_lines = []
    with open(CONFIG_DAT_PATH, 'r') as f:
        lines = f.readlines()
        for line in lines:
            for k, v in j.iteritems():
                if line.strip().startswith(k):
                    line = '    ' + k + ' = ' + v + '\n'
            new_lines.append(line.rstrip() + '\n')
    with open(CONFIG_DAT_PATH, 'w') as f:
        f.writelines(new_lines)
        f.flush()


def run():
    try:
        while True:
            now = datetime.now().strftime('%H:%M')
            if now in CONFIG_JSON['run_moments_everyday']:
                print datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                for upload_config in CONFIG_JSON['upload_configs']:
                    change_config(upload_config[0], upload_config[1], upload_config[2])
                    do_task()
                print 'Waiting for next run moment...'
                while True:
                    if datetime.now().strftime('%H:%M') == now:
                        time.sleep(0.1)
                    else:
                        break
            else:
                time.sleep(0.1)
    except KeyboardInterrupt:
        print 'exit...'
        if task:
            try:
                task.terminate()
            except OSError:
                pass
        exit()


if __name__ == '__main__':
    run()
