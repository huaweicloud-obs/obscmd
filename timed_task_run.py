#!/usr/bin/env python
# -*- coding:utf-8 -*-
from subprocess import *
from timed_task_config import *
from datetime import datetime
import time
import os

CMD = 'python ' + os.path.join(os.path.split(os.path.abspath(__file__))[0], 'run.py')


def do_task():
    p = Popen(CMD, shell=True, stdout=PIPE, stdin=PIPE, stderr=PIPE)
    out, err = p.communicate()
    print str(datetime.now()) + ': '
    if err:
        print 'Error: ' + err
    else:
        print "Executed '" + CMD + "'"
        print out


def run():
    while True:
        if datetime.now().strftime('%H:%M') == EXECUTE_TIME_EVERY_DAY.replace('：', ':'):
            do_task()
            time.sleep(23 * 60 * 60)
        else:
            time.sleep(1)


if __name__ == '__main__':
    run()
