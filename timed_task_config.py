# -*- coding:utf-8 -*-
"""
CONFIG_JSON = {
    'run_moments_everyday': ['8:00', '12:30'],
    'upload_config': [
        ('LocalPath1', 'bucket1', 'RemoteDir1'),
        ('LocalPath2', 'bucket2', 'RemoteDir2')
    ]
}
run_time_everyday: list of moments([HH:MM, HH:MM]). The intervals must be at least 1 min.
upload_config: LocalPath, bucket and RemoteDir for uploading, list of tuples(LocalPath, bucket, RemoteDir).

Make sure other items in the config.dat are fully configured.
Running the timed_task_run.py will change the config.dat, so please check the config.dat next time.
"""

CONFIG_JSON = {
    'run_moments_everyday': ['8:00', '12:30'],
    'upload_configs': [
        ('LocalPath1', 'bucket1', 'RemoteDir1'),
        ('LocalPath2', 'bucket2', 'RemoteDir2')
    ]
}
