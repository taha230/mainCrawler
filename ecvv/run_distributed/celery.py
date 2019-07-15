from __future__ import absolute_import
from celery import Celery
####################################################
app = Celery('run_distributed',broker='pyamqp://seyyed:1586567@192.168.1.5/seyyed_host',backend='rpc://',include=['run_distributed.tasks'])


app.conf.update(task_track_started=True, result_backend='file:///home/taha/Desktop/ecvv_distributed/run_distributed/results')

if __name__ == '__main__':
    app.start()


