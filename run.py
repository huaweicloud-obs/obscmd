#!/usr/bin/env python
# -*- coding:utf-8 -*-
import Queue
import base64
import hashlib
import logging
import logging.config
import multiprocessing
import os
import sys
import threading
import time
import traceback
import Util
import obsPyCmd
import results
import myLib.cloghandler
from copy import deepcopy
from Queue import Empty
from constant import ConfigFile
from constant import LOCAL_SYS
from constant import SYS_ENCODING
from constant import CONTENT_TYPES
from Util import Counter
from Util import ThreadsStopFlag
from Util import RangeFileWriter

ThreadQueue = Queue.Queue

if LOCAL_SYS == 'windows':
    Worker = threading.Thread
    Queue = Queue.Queue
    Lock = threading.Lock
else:
    Worker = multiprocessing.Process
    Queue = multiprocessing.Queue
    Lock = multiprocessing.Lock

VERSION = 'v4.2.1'
# running config
CONFIG = {}
RETRY_TIMES = 3
UPLOAD_PART_MIN_SIZE = 5 * 1024 ** 2
UPLOAD_PART_MAX_SIZE = 5 * 1024 ** 3
TEST_CASES = {
    201: 'PutObject;put_object',
    202: 'GetObject;get_object'
}
user = None
all_files_queue = Queue()
all_objects_queue = Queue()
big_obj_tuple_list_to_range_download_for_windows = []
results_queue = multiprocessing.Queue()
lock = Lock()
current_concurrency = multiprocessing.Value('i', 0)
total_data = multiprocessing.Value('f', 0)
total_data_upload = 0
total_data_download = 0


class User:
    def __init__(self, username, ak, sk):
        self.username = username
        self.ak = ak
        self.sk = sk


def read_config(config_file_name=ConfigFile.FILE_CONFIG):
    global user
    try:
        f = open(config_file_name, 'r')
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            if line and line[0] != '#':
                CONFIG[line[:line.find('=')].strip()] = line[line.find('=') + 1:].strip()
            else:
                continue
        f.close()

        CONFIG['AK'] = prompt_for_input('AK', 'your account')
        CONFIG['SK'] = prompt_for_input('SK', 'your account')
        user = User('obscmd', CONFIG['AK'], CONFIG['SK'])
        # Don't show SK on screen display
        del CONFIG['SK']

        if CONFIG['IsHTTPs'].lower() == 'true':
            CONFIG['IsHTTPs'] = True
        else:
            CONFIG['IsHTTPs'] = False

        CONFIG['ConnectTimeout'] = int(CONFIG['ConnectTimeout'])
        if int(CONFIG['ConnectTimeout']) < 5:
            CONFIG['ConnectTimeout'] = 5

        if CONFIG['RemoteDir']:
            CONFIG['RemoteDir'] = CONFIG['RemoteDir'].replace('\\', '/').strip('/')
            if CONFIG['RemoteDir']:
                CONFIG['RemoteDir'] = CONFIG['RemoteDir'] + '/'

        CONFIG['DownloadTarget'] = CONFIG['DownloadTarget'].lstrip('/')

        if CONFIG['VirtualHost'].lower() == 'true':
            CONFIG['VirtualHost'] = True
        else:
            CONFIG['VirtualHost'] = False

        if CONFIG['RecordDetails'].lower() == 'true':
            CONFIG['RecordDetails'] = True
        else:
            CONFIG['RecordDetails'] = False

        if CONFIG['BadRequestCounted'].lower() == 'true':
            CONFIG['BadRequestCounted'] = True
        else:
            CONFIG['BadRequestCounted'] = False

        if CONFIG['PrintProgress'].lower() == 'true':
            CONFIG['PrintProgress'] = True
        else:
            CONFIG['PrintProgress'] = False

        if CONFIG['IgnoreExist'].lower() == 'true':
            CONFIG['IgnoreExist'] = True
        else:
            CONFIG['IgnoreExist'] = False

        if CONFIG['CompareETag'].lower() == 'true':
            CONFIG['CompareETag'] = True
        else:
            CONFIG['CompareETag'] = False

        # User's input
        CONFIG['Operation'] = prompt_for_input('Operation', 'operation(upload/download)')
        if not CONFIG['Operation'].lower() == 'upload' and not CONFIG['Operation'].lower() == 'download':
            print 'Operation must be upload or download, exit...'
            exit()

        if CONFIG['Operation'].lower() == 'upload':
            CONFIG['Testcase'] = 201
        elif CONFIG['Operation'].lower() == 'download':
            CONFIG['Testcase'] = 202

        if CONFIG.get('MultipartObjectSize'):
            CONFIG['PartSize'] = prompt_for_input('PartSize', 'multipart size')
            if CONFIG['Operation'].lower() == 'upload' and int(CONFIG['PartSize']) < UPLOAD_PART_MIN_SIZE:
                CONFIG['PartSize'] = str(UPLOAD_PART_MIN_SIZE)
            if CONFIG['Operation'].lower() == 'upload' and int(CONFIG['PartSize']) > UPLOAD_PART_MAX_SIZE:
                CONFIG['PartSize'] = str(UPLOAD_PART_MAX_SIZE)
            if int(CONFIG['PartSize']) > int(CONFIG.get('MultipartObjectSize')):
                print 'In order to cut object(s) to pieces, PartSize must be less than MultipartObjectSize'
                exit()
        else:
            CONFIG['MultipartObjectSize'] = '0'

        CONFIG['Concurrency'] = int(CONFIG['Concurrency']) if CONFIG['Concurrency'] else 0
        CONFIG['MixLoopCount'] = 1
        CONFIG['LongConnection'] = False
        CONFIG['ConnectionHeader'] = ''
        CONFIG['AvoidSinBkOp'] = True
        CONFIG['CollectBasicData'] = False
        CONFIG['IsMaster'] = False
        CONFIG['IsFromLocal'] = True
        CONFIG['Mode'] = '1'
        CONFIG['LatencyRequestsNumber'] = False
        CONFIG['LatencyPercentileMap'] = False
        CONFIG['StatisticsInterval'] = 3
        CONFIG['LatencySections'] = '500,1000,3000,10000'

        # If server side encryption is on, set https + AWSV4 on.
        if CONFIG['SrvSideEncryptType']:
            if not CONFIG['IsHTTPs']:
                CONFIG['IsHTTPs'] = True
                logging.warning('change IsHTTPs to True while use SrvSideEncryptType')
            if CONFIG['AuthAlgorithm'] != 'AWSV4' and CONFIG['SrvSideEncryptType'].lower() == 'sse-kms':
                CONFIG['AuthAlgorithm'] = 'AWSV4'
                logging.warning('change AuthAlgorithm to AWSV4 while use SrvSideEncryptType = SSE-KMS')
    except Exception, data:
        print '[ERROR] Read config file %s error: %s' % (config_file_name, data)
        sys.exit()


def initialize_object_name(target_in_local, keys_already_exist_list):
    global total_data_upload

    remote_dir = CONFIG['RemoteDir']
    multi_part_object_size = int(CONFIG.get('MultipartObjectSize'))
    part_size = int(CONFIG['PartSize'])

    def generate_task_tuple(file_path):
        global total_data_upload
        file_path = file_path.strip()
        if not os.path.isfile(file_path):
            print '{target} is not a file. Skip it.'.format(target=file_path)
        else:
            key = os.path.split(file_path)[1]

            if remote_dir:
                key = remote_dir + key

            task_tuple = None
            if key.decode(SYS_ENCODING) not in keys_already_exist_list:
                size = int(os.path.getsize(file_path))
                total_data_upload += size
                if size >= multi_part_object_size:
                    parts = size / part_size + 1
                    if parts > 10000:
                        msg_t = 'PartSize({part_size}) is too small.\n' \
                                'You have a file({file}) cut to more than 10,000 parts.\n' \
                                'Please make sure every file is cut to less than or equal to 10,000 parts. Exit...' \
                            .format(part_size=CONFIG['PartSize'], file=key)
                        print msg_t
                        logging.warn(msg_t)
                        exit()

                task_tuple = (key, size, file_path)
            return task_tuple

    if ',' not in target_in_local:
        if os.path.isdir(target_in_local):
            files = []
            try:
                files = os.listdir(target_in_local)
            except OSError:
                pass

            for fi in files:
                fi_d = os.path.join(target_in_local, fi)
                if os.path.isdir(fi_d):
                    logging.warn('scanning dir: ' + fi_d)
                    initialize_object_name(fi_d, keys_already_exist_list)
                elif os.path.isfile(fi_d):
                    object_to_put = fi_d.replace(CONFIG['LocalPath'], '')
                    if LOCAL_SYS == 'windows':
                        object_to_put = object_to_put.replace('\\', '/')
                    object_to_put = object_to_put.lstrip('/')

                    if remote_dir:
                        object_to_put = remote_dir + object_to_put

                    if object_to_put.decode(SYS_ENCODING) not in keys_already_exist_list:
                        object_size = int(os.path.getsize(fi_d))
                        total_data_upload += object_size
                        if object_size >= multi_part_object_size:
                            parts_count = object_size / part_size + 1
                            if parts_count > 10000:
                                msg = 'PartSize({part_size}) is too small.\n' \
                                      'You have a file({file}) cut to more than 10,000 parts.\n' \
                                      'Please make sure every file is cut to less than or equal to 10,000 parts.\n' \
                                      'Exit...' \
                                    .format(part_size=CONFIG['PartSize'], file=object_to_put)
                                print msg
                                logging.warn(msg)
                                exit()

                        all_files_queue.put((object_to_put, object_size, fi_d))
        elif os.path.isfile(target_in_local):
            task_t = generate_task_tuple(target_in_local)
            if task_t:
                all_files_queue.put(task_t)
    else:
        targets = target_in_local.split(',')
        targets = list(set(targets))
        for target in targets:
            task_t = generate_task_tuple(target)
            if task_t:
                all_files_queue.put(task_t)


def get_all_keys_in_bucket(bucket_name, ak, sk, target_in_bucket=''):
    from xml.etree import ElementTree
    m = 'getting keys in bucket...'
    print m
    logging.warn(m)
    lists_objects = []
    targets = list(set(target_in_bucket.split(',')))
    for target in targets:
        target = target.strip()
        list_objects = []
        marker = ''
        if target:
            if LOCAL_SYS == 'windows':
                target = target.replace('\\', '/')

        while marker is not None:
            conn = obsPyCmd.MyHTTPConnection(host=CONFIG['DomainName'],
                                             is_secure=CONFIG['IsHTTPs'],
                                             ssl_version=CONFIG['sslVersion'],
                                             timeout=CONFIG['ConnectTimeout'],
                                             long_connection=CONFIG['LongConnection'],
                                             conn_header=CONFIG['ConnectionHeader'])
            rest = obsPyCmd.OBSRequestDescriptor(request_type='ListObjectsInBucket',
                                                 ak=ak, sk=sk,
                                                 auth_algorithm=CONFIG['AuthAlgorithm'],
                                                 virtual_host=CONFIG['VirtualHost'],
                                                 domain_name=CONFIG['DomainName'],
                                                 region=CONFIG['Region'])
            rest.bucket = bucket_name

            # List a directory
            if target.endswith('/'):
                dir_prefix = target.strip('/')
                if dir_prefix:
                    dir_prefix = dir_prefix + '/'
                    rest.queryArgs['prefix'] = dir_prefix
            elif target.endswith('*'):
                prefix = target.strip('/').rstrip('*')
                if prefix:
                    rest.queryArgs['prefix'] = prefix
            # List an object
            elif target:
                rest.queryArgs['prefix'] = target

            if marker:
                rest.queryArgs['marker'] = marker

            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
            marker = resp.return_data
            xml_body = resp.recv_body

            if not xml_body:
                print 'Error in http request, please see log/*.log'
                exit()

            if '<Code>NoSuchBucket</Code>' in xml_body:
                print 'No such bucket(%s), exit...' % bucket_name
                logging.warn('No such bucket(%s), exit...' % bucket_name)
                exit()

            root = ElementTree.fromstring(xml_body)
            if '<Contents>' in xml_body:
                if '<NextMarker>' in xml_body:
                    for contents_element in root[6:]:
                        if contents_element[0].text[-1] != '/':
                            list_objects.append((contents_element[0].text, int(contents_element[3].text)))
                else:
                    for contents_element in root[5:]:
                        if contents_element[0].text[-1] != '/':
                            list_objects.append((contents_element[0].text, int(contents_element[3].text)))

        # If target is a single object, check if it's in the bucket.
        if target and not target.endswith(('/', '*')):
            find_flag = False
            for one_tuple in list_objects:
                if target == one_tuple[0]:
                    find_flag = True
                    break
            if not find_flag:
                list_objects = []
        lists_objects.extend(list_objects)
    return list(set(lists_objects))


def put_object(worker_id, conn):
    while not all_files_queue.empty():
        try:
            file_tuple = all_files_queue.get(block=False)
            logging.info('put_object tuple:' + str(file_tuple))
        except Empty:
            continue
        if file_tuple[1] < int(CONFIG.get('MultipartObjectSize')):
            rest = obsPyCmd.OBSRequestDescriptor(request_type='PutObject',
                                                 ak=user.ak, sk=user.sk,
                                                 auth_algorithm=CONFIG['AuthAlgorithm'],
                                                 virtual_host=CONFIG['VirtualHost'],
                                                 domain_name=CONFIG['DomainName'],
                                                 region=CONFIG['Region'])
            try:
                rest.key = file_tuple[0].decode(SYS_ENCODING).encode('utf8')
            except UnicodeDecodeError:
                logging.warn('Decode error, key: ' + file_tuple[0])
                continue
            rest.bucket = CONFIG['BucketNameFixed']

            rest.headers['content-type'] = 'application/octet-stream'
            tokens = file_tuple[0].split('.')
            if len(tokens) > 1:
                suffix = tokens[-1].strip().lower()
                if suffix in CONTENT_TYPES:
                    rest.headers['content-type'] = CONTENT_TYPES[suffix]

            if CONFIG['PutWithACL']:
                rest.headers['x-amz-acl'] = CONFIG['PutWithACL']
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(
                    CONFIG['CustomerKey'])
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(CONFIG['CustomerKey']).digest())
            elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' \
                    and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aws:kms':
                rest.headers['x-amz-server-side-encryption'] = 'aws:kms'
                if CONFIG['SrvSideEncryptAWSKMSKeyId']:
                    rest.headers['x-amz-server-side-encryption-aws-kms-key-id'] = CONFIG['SrvSideEncryptAWSKMSKeyId']
                if CONFIG['SrvSideEncryptContext']:
                    rest.headers['x-amz-server-side-encryption-context'] = CONFIG['SrvSideEncryptContext']
            elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' \
                    and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aes256':
                rest.headers['x-amz-server-side-encryption'] = 'AES256'

            file_location = file_tuple[2]
            rest.contentLength = file_tuple[1]

            retry_count = 0
            resp = obsPyCmd.DefineResponse()
            resp.status = '99999 Not Ready'
            md5 = ''
            e_tag = ''
            e_tag_record = ''

            while not resp.status.startswith('20') or md5 != e_tag:
                resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(file_location=file_location)

                e_tag = resp.e_tag
                if CONFIG['CompareETag']:
                    if not md5:
                        md5 = Util.calculate_file_md5(file_location=file_location)
                e_tag_record = 'ETag: local(%s) server(%s)' % (md5, e_tag)

                if not resp.status.startswith('20'):
                    if retry_count == RETRY_TIMES:
                        logging.warn('Max retry put_object, key: %s. Status is %s' %
                                     (rest.key, resp.status))
                        break
                    retry_count += 1
                    logging.warn('Status is %s. Retry put_object, key: %s, retry_count: %d' %
                                 (resp.status, rest.key, retry_count))
                    time.sleep(5)
                elif md5 != e_tag:
                    if retry_count == RETRY_TIMES:
                        logging.warn('Max retry put_object, key: %s. Compare md5 error, %s.' %
                                     (rest.key, e_tag_record))
                        break
                    retry_count += 1
                    logging.warn('Compare md5 error, %s. Retry put_object, key: %s, retry_count: %d' %
                                 (e_tag_record, rest.key, retry_count))
                    time.sleep(5)
                else:
                    break
            results_queue.put(
                (worker_id, user.username, rest.recordUrl, rest.requestType, resp.start_time, resp.end_time,
                 resp.send_bytes, 0, e_tag_record, resp.request_id, resp.status, resp.id2))
        else:
            rest = obsPyCmd.OBSRequestDescriptor(request_type='',
                                                 ak=user.ak, sk=user.sk,
                                                 auth_algorithm=CONFIG['AuthAlgorithm'],
                                                 virtual_host=CONFIG['VirtualHost'],
                                                 domain_name=CONFIG['DomainName'],
                                                 region=CONFIG['Region'])
            try:
                rest.key = file_tuple[0].decode(SYS_ENCODING).encode('utf8')
            except UnicodeDecodeError:
                logging.warn('Decode error, key: ' + file_tuple[0])
                continue
            rest.bucket = CONFIG['BucketNameFixed']
            process_multi_parts_upload(file_tuple=file_tuple,
                                       rest=rest,
                                       conn=conn,
                                       worker_id=worker_id)


def get_object(worker_id, conn):
    while not all_objects_queue.empty():
        try:
            object_tuple = all_objects_queue.get(block=False)
        except Empty:
            continue

        if object_tuple[1] < int(CONFIG.get('MultipartObjectSize')):
            rest = obsPyCmd.OBSRequestDescriptor(request_type='GetObject',
                                                 ak=user.ak, sk=user.sk,
                                                 auth_algorithm=CONFIG['AuthAlgorithm'],
                                                 virtual_host=CONFIG['VirtualHost'],
                                                 domain_name=CONFIG['DomainName'],
                                                 region=CONFIG['Region'])
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'

            save_path_parent = CONFIG['SavePath']
            rest.bucket = CONFIG['BucketNameFixed']
            rest.key = object_tuple[0].encode('utf8')
            file_name = object_tuple[0].encode(SYS_ENCODING)

            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(CONFIG['CustomerKey'])
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(CONFIG['CustomerKey']).digest())
            resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request(save_path_parent=save_path_parent,
                                                                       file_name=file_name)
            md5 = ''
            e_tag = resp.e_tag
            e_tag_record = 'ETag: local(%s) server(%s)' % (md5, e_tag)

            results_queue.put(
                (worker_id, user.username, rest.recordUrl, rest.requestType, resp.start_time, resp.end_time, 0,
                 resp.recv_bytes, e_tag_record, resp.request_id, resp.status, resp.id2)
            )
        else:
            rest = obsPyCmd.OBSRequestDescriptor(request_type='GetObject',
                                                 ak=user.ak, sk=user.sk,
                                                 auth_algorithm=CONFIG['AuthAlgorithm'],
                                                 virtual_host=CONFIG['VirtualHost'],
                                                 domain_name=CONFIG['DomainName'],
                                                 region=CONFIG['Region'])
            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'

            save_path_parent = CONFIG['SavePath']
            rest.bucket = CONFIG['BucketNameFixed']
            rest.key = object_tuple[0].encode('utf8')

            if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
                rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(CONFIG['CustomerKey'])
                rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                    hashlib.md5(CONFIG['CustomerKey']).digest())
            process_range_download(rest, object_tuple, save_path_parent, worker_id)


def process_range_download(rest, object_tuple, save_path_parent, worker_id):
    size_to_download = object_tuple[1]
    part_size = int(CONFIG['PartSize'])
    total_parts = size_to_download / part_size + 1
    parts_count = 0
    range_start = 0
    th_list = []
    counter = Counter()
    stop_flag_obj = ThreadsStopFlag()
    lock_t = threading.Lock()
    data_queue = ThreadQueue(1024)
    file_name = object_tuple[0].encode(SYS_ENCODING)
    file_path = os.path.join(save_path_parent, file_name)
    dir_path = os.path.dirname(file_path)

    if not os.path.isdir(dir_path):
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    range_file_writer = RangeFileWriter(data_queue, file_path, total_parts)
    range_file_writer.daemon = True
    range_file_writer.start()

    while size_to_download:
        if stop_flag_obj.flag:
            total_data.value -= size_to_download
            break
        if counter.count > 0 and current_concurrency.value >= CONFIG['Concurrency']:
            time.sleep(0.5)
            continue

        rest_in = deepcopy(rest)
        range_start_in = deepcopy(range_start)
        if size_to_download >= part_size:
            rest_in.headers['Range'] = 'bytes=%d-%d' % (range_start, range_start + (part_size - 1))
            range_start += part_size
            size_to_download -= part_size
        else:
            rest_in.headers['Range'] = 'bytes=%d-%d' % (range_start, object_tuple[1] - 1)
            size_to_download = 0

        def run_download_part(rest_t, range_start_t):
            temp_conn = None
            try:
                temp_conn = obsPyCmd.MyHTTPConnection(host=CONFIG['DomainName'],
                                                      is_secure=CONFIG['IsHTTPs'],
                                                      ssl_version=CONFIG['sslVersion'],
                                                      timeout=CONFIG['ConnectTimeout'],
                                                      long_connection=CONFIG['LongConnection'],
                                                      conn_header=CONFIG['ConnectionHeader'])
                retry_count = 0
                resp = obsPyCmd.DefineResponse()
                resp.status = '99999 Not Ready'
                is_last_retry = False
                md5 = ''
                e_tag_record = ''
                while not resp.status.startswith('20'):
                    if retry_count == RETRY_TIMES:
                        is_last_retry = True
                    resp = obsPyCmd.OBSRequestHandler(rest_t, temp_conn).make_request(is_range_download=True,
                                                                                      range_start=range_start_t,
                                                                                      part_download_queue=data_queue,
                                                                                      stop_flag_obj=stop_flag_obj,
                                                                                      is_last_retry=is_last_retry)
                    e_tag = resp.e_tag
                    e_tag_record = 'ETag: local(%s) server(%s)' % (md5, e_tag)
                    if not resp.status.startswith('20'):
                        if retry_count == RETRY_TIMES:
                            logging.warn('Max retry download_part, key: %s, range start: %d. Status is %s.' %
                                         (rest.key, range_start_t, resp.status))
                            break
                        retry_count += 1
                        logging.warn('Status is %s. Retry download_part, key: %s, range start: %d, retry_count: %d' %
                                     (resp.status, rest.key, range_start_t, retry_count))
                        time.sleep(5)
                    else:
                        break
                results_queue.put(
                    (worker_id, user.username, rest_t.recordUrl, rest_t.requestType, resp.start_time,
                     resp.end_time, 0, resp.recv_bytes, e_tag_record, resp.request_id, resp.status, resp.id2))
            except Exception:
                stack = traceback.format_exc()
                logging.warn('range_download exception stack: %s' % stack)
            finally:
                if temp_conn:
                    temp_conn.close_connection()
                lock_t.acquire()
                counter.count -= 1
                lock_t.release()

                if counter.count > 0:
                    lock.acquire()
                    current_concurrency.value -= 1
                    lock.release()

        th = threading.Thread(target=run_download_part, args=(rest_in, range_start_in))
        th.daemon = True
        th.start()

        lock_t.acquire()
        counter.count += 1
        lock_t.release()

        if counter.count > 1:
            lock.acquire()
            current_concurrency.value += 1
            lock.release()

        th_list.append(th)

        parts_count += 1

    for th in th_list:
        th.join()
    range_file_writer.join()


def range_download_for_list(big_obj_tuple_to_download):
    current_concurrency.value += 1

    worker_id = 9999

    rest = obsPyCmd.OBSRequestDescriptor(request_type='GetObject',
                                         ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'],
                                         virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'],
                                         region=CONFIG['Region'])
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'

    save_path_parent = CONFIG['SavePath']
    rest.bucket = CONFIG['BucketNameFixed']

    for object_tuple in big_obj_tuple_to_download:
        rest.key = object_tuple[0].encode('utf8')

        if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
            rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(rest.key[-32:].zfill(32))
            rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                hashlib.md5(rest.key[-32:].zfill(32)).digest())
        process_range_download(rest, object_tuple, save_path_parent, worker_id)

    current_concurrency.value -= 1


def process_multi_parts_upload(file_tuple, rest, conn, worker_id):
    # 1. Initiate multipart upload
    object_path = file_tuple[2]
    size_to_put = file_tuple[1]
    rest.requestType = 'InitMultiUpload'
    rest.method = 'POST'
    rest.headers = {}
    rest.queryArgs = {}
    rest.sendContent = ''
    rest.contentLength = 0

    rest.headers['content-type'] = 'application/octet-stream'
    tokens = file_tuple[0].split('.')
    if len(tokens) > 1:
        suffix = tokens[-1].strip().lower()
        if suffix in CONTENT_TYPES:
            rest.headers['content-type'] = CONTENT_TYPES[suffix]

    rest.queryArgs['uploads'] = None
    if CONFIG['PutWithACL']:
        rest.headers['x-amz-acl'] = CONFIG['PutWithACL']
    if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
        rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
        rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(
            CONFIG['CustomerKey'])
        rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
            hashlib.md5(CONFIG['CustomerKey']).digest())
    elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aws:kms':
        rest.headers['x-amz-server-side-encryption'] = 'aws:kms'
        if CONFIG['SrvSideEncryptAWSKMSKeyId']:
            rest.headers['x-amz-server-side-encryption-aws-kms-key-id'] = CONFIG[
                'SrvSideEncryptAWSKMSKeyId']
        if CONFIG['SrvSideEncryptContext']:
            rest.headers['x-amz-server-side-encryption-context'] = CONFIG['SrvSideEncryptContext']
    elif CONFIG['SrvSideEncryptType'].lower() == 'sse-kms' and CONFIG['SrvSideEncryptAlgorithm'].lower() == 'aes256':
        rest.headers['x-amz-server-side-encryption'] = 'AES256'

    resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
    results_queue.put(
        (worker_id, user.username, rest.recordUrl, rest.requestType, resp.start_time,
         resp.end_time, 0, 0, '', resp.request_id, resp.status, resp.id2))

    if not resp.status.startswith('20'):
        # If initiating multipart upload failed, return.
        logging.warn('key: ' + rest.key + ', initiate multipart upload failed')
        total_data.value -= size_to_put
        return

    upload_id = resp.return_data
    logging.info("upload id: %s" % upload_id)
    # 2. Upload multipart concurrently
    rest.requestType = 'UploadPart'
    rest.method = 'PUT'
    rest.headers = {}
    rest.queryArgs = {}
    rest.sendContent = ''
    rest.queryArgs['uploadId'] = upload_id
    part_number = 1
    part_etags = {}
    part_index = 0
    part_size = int(CONFIG['PartSize'])
    th_list = []
    counter = Counter()
    stop_flag_obj = ThreadsStopFlag()
    lock_t = threading.Lock()
    while size_to_put:
        if stop_flag_obj.flag:
            total_data.value -= size_to_put
            for th in th_list:
                th.join()
            return
        if counter.count > 0 and current_concurrency.value >= CONFIG['Concurrency']:
            time.sleep(0.5)
            continue
        if size_to_put >= part_size:
            rest.contentLength = part_size
        else:
            rest.contentLength = size_to_put

        rest.queryArgs['partNumber'] = str(part_number)

        if CONFIG['SrvSideEncryptType'].lower() == 'sse-c':
            rest.headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
            rest.headers['x-amz-server-side-encryption-customer-key'] = base64.b64encode(
                CONFIG['CustomerKey'])
            rest.headers['x-amz-server-side-encryption-customer-key-MD5'] = base64.b64encode(
                hashlib.md5(CONFIG['CustomerKey']).digest())

        def run_upload_part(rest_t, part_number_t, part_index_t):
            temp_conn = None
            try:
                temp_conn = obsPyCmd.MyHTTPConnection(host=CONFIG['DomainName'],
                                                      is_secure=CONFIG['IsHTTPs'],
                                                      ssl_version=CONFIG['sslVersion'],
                                                      timeout=CONFIG['ConnectTimeout'],
                                                      long_connection=CONFIG['LongConnection'],
                                                      conn_header=CONFIG['ConnectionHeader'])
                retry_count = 0
                resp_in = obsPyCmd.DefineResponse()
                resp_in.status = '99999 Not Ready'
                md5 = ''
                e_tag = ''
                e_tag_record = ''
                while not resp_in.status.startswith('20') or md5 != e_tag:
                    resp_in = obsPyCmd.OBSRequestHandler(rest_t, temp_conn).make_request(is_part_upload=True,
                                                                                         part_index=part_index_t,
                                                                                         file_location=object_path,
                                                                                         stop_flag_obj=stop_flag_obj)
                    e_tag = resp_in.e_tag
                    if CONFIG['CompareETag']:
                        if not md5:
                            md5 = Util.calculate_file_md5(file_location=object_path,
                                                          part_start=part_index_t,
                                                          part_size=rest_t.contentLength)
                    e_tag_record = 'ETag: local(%s) server(%s)' % (md5, e_tag)

                    if not resp_in.status.startswith('20'):
                        if retry_count == RETRY_TIMES:
                            stop_flag_obj.flag = True
                            logging.warn('Max retry upload_part, key: %s, part_index: %d, status: %s' %
                                         (rest.key, part_index_t, resp_in.status))
                            break
                        retry_count += 1
                        logging.warn('Retry upload_part, key: %s, part_index: %d, retry_count: %d, status: %s' %
                                     (rest.key, part_index_t, retry_count, resp_in.status))
                        time.sleep(5)
                    elif md5 != e_tag:
                        if retry_count == RETRY_TIMES:
                            logging.warn('Max retry upload_part, key: %s, part_index: %d. Compare md5 error, %s.' %
                                         (rest.key, part_index_t, e_tag_record))
                            break
                        retry_count += 1
                        logging.warn('Compare md5 error, %s. Retry upload_part, key: %s, part_index: %d, '
                                     'retry_count: %d' % (e_tag_record, rest.key, part_index_t, retry_count))
                        time.sleep(5)
                    else:
                        break
                results_queue.put(
                    (worker_id, user.username, rest_t.recordUrl, rest_t.requestType, resp_in.start_time,
                     resp_in.end_time, resp_in.send_bytes, 0, e_tag_record, resp_in.request_id, resp_in.status,
                     resp.id2))

                if resp_in.status.startswith('20'):
                    part_etags[part_number_t] = resp_in.e_tag
                else:
                    # If some part failed, inform this worker to cancel complete multipart upload.
                    part_etags[part_number_t] = False
            except Exception:
                stack = traceback.format_exc()
                logging.warn('multi_parts_upload exception stack: %s' % stack)
            finally:
                if temp_conn:
                    temp_conn.close_connection()
                lock_t.acquire()
                counter.count -= 1
                lock_t.release()

                if counter.count > 0:
                    lock.acquire()
                    current_concurrency.value -= 1
                    lock.release()

        rest_in = deepcopy(rest)
        part_number_in = deepcopy(part_number)
        part_index_in = deepcopy(part_index)
        th = threading.Thread(target=run_upload_part, args=(rest_in, part_number_in, part_index_in))
        th.daemon = True
        th.start()

        lock_t.acquire()
        counter.count += 1
        lock_t.release()

        if counter.count > 1:
            lock.acquire()
            current_concurrency.value += 1
            lock.release()

        th_list.append(th)

        part_number += 1
        size_to_put -= rest.contentLength
        part_index += rest.contentLength

    for th in th_list:
        th.join()

    # If some part failed, cancel complete multipart upload.
    for key, value in part_etags.iteritems():
        if value is False:
            return

    # 3. Complete multipart upload
    rest.requestType = 'CompleteMultiUpload'
    rest.method = 'POST'
    rest.headers = {}
    rest.queryArgs = {}
    rest.contentLength = 0
    rest.headers['content-type'] = 'application/xml'
    rest.queryArgs['uploadId'] = upload_id
    rest.sendContent = '<CompleteMultipartUpload>'
    for part_index in sorted(part_etags):
        rest.sendContent += '<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>' % (
            part_index, part_etags[part_index])
    rest.sendContent += '</CompleteMultipartUpload>'
    resp = obsPyCmd.OBSRequestHandler(rest, conn).make_request()
    results_queue.put(
        (worker_id, user.username, rest.recordUrl, rest.requestType, resp.start_time,
         resp.end_time, 0, 0, '', resp.request_id, resp.status, resp.id2))


def multi_parts_upload_for_list(big_obj_tuple_to_upload):
    current_concurrency.value += 1

    worker_id = 9999
    conn = obsPyCmd.MyHTTPConnection(host=CONFIG['DomainName'],
                                     is_secure=CONFIG['IsHTTPs'],
                                     ssl_version=CONFIG['sslVersion'],
                                     timeout=CONFIG['ConnectTimeout'],
                                     long_connection=CONFIG['LongConnection'],
                                     conn_header=CONFIG['ConnectionHeader'])

    rest = obsPyCmd.OBSRequestDescriptor(request_type='',
                                         ak=user.ak, sk=user.sk,
                                         auth_algorithm=CONFIG['AuthAlgorithm'],
                                         virtual_host=CONFIG['VirtualHost'],
                                         domain_name=CONFIG['DomainName'],
                                         region=CONFIG['Region'])
    rest.bucket = CONFIG['BucketNameFixed']
    for large_object_tuple in big_obj_tuple_to_upload:
        try:
            rest.key = large_object_tuple[0].decode(SYS_ENCODING).encode('utf8')
        except UnicodeDecodeError:
            logging.warn('Decode error, key: ' + large_object_tuple[0])
            continue
        process_multi_parts_upload(file_tuple=large_object_tuple,
                                   rest=rest,
                                   conn=conn,
                                   worker_id=worker_id)

    current_concurrency.value -= 1
    conn.close_connection()


# Start worker
def start_worker(worker_id, test_case, valid_start_time, valid_end_time, conn=None, call_itself=False):
    if not call_itself:
        lock.acquire()
        current_concurrency.value += 1
        lock.release()

    if not conn:
        conn = obsPyCmd.MyHTTPConnection(host=CONFIG['DomainName'],
                                         is_secure=CONFIG['IsHTTPs'],
                                         ssl_version=CONFIG['sslVersion'],
                                         timeout=CONFIG['ConnectTimeout'],
                                         long_connection=CONFIG['LongConnection'],
                                         conn_header=CONFIG['ConnectionHeader'])
    if test_case != 900:
        try:
            method_to_call = globals()[TEST_CASES[test_case].split(';')[1]]
            logging.debug('method %s called ' % method_to_call.__name__)
            method_to_call(worker_id, conn)
        except KeyboardInterrupt:
            pass
        except Exception:
            stack = traceback.format_exc()
            logging.warn('Call method for test case %d except: %s' % (test_case, stack))
    elif test_case == 900:
        test_cases = [int(case) for case in CONFIG['MixOperations'].split(',')]
        tmp = 0
        while tmp < CONFIG['MixLoopCount']:
            logging.debug("loop count: %d " % tmp)
            tmp += 1
            for case in test_cases:
                logging.debug("case %d in mix loop called " % case)
                start_worker(worker_id, case, valid_start_time, valid_end_time, conn, True)

    # If call the def self(MixOperation), return and not close connection.
    if call_itself:
        return

    # Close connection for this worker
    if conn:
        conn.close_connection()

    lock.acquire()
    current_concurrency.value -= 1
    lock.release()
    logging.info('worker_id [%d] exit, set current_concurrency.value = %d' % (worker_id, current_concurrency.value))
    # When all concurrent workers finish, set the business end time
    if current_concurrency.value == 0:
        valid_end_time.value = time.time()
        logging.info('worker [' + str(worker_id) + '], exit, set valid_end_time = ' + str(valid_end_time.value))


def get_total_data_size():
    if CONFIG['Operation'].lower() == 'download':
        total_data.value = total_data_download
        return total_data
    elif CONFIG['Operation'].lower() == 'upload':
        total_data.value = total_data_upload
        return total_data
    else:
        return -1


# return True: pass, False: failed
def precondition():
    # Check if the user is root.
    import getpass

    if LOCAL_SYS.lower().startswith('linux') and 'root' != getpass.getuser():
        return False, "\033[1;31;40m%s\033[0m Please run with root account other than '%s'" % (
            "[ERROR]", getpass.getuser())

    # Check if the test case is set right.
    if CONFIG['Testcase'] not in TEST_CASES:
        return False, "\033[1;31;40m%s\033[0m Test Case [%d] not supported" % ("[ERROR]", CONFIG['Testcase'])

    # Test connection
    if CONFIG['IsHTTPs']:
        try:
            import ssl
            if not CONFIG['sslVersion']:
                CONFIG['sslVersion'] = 'SSLv23'
            logging.info('import ssl module done, config ssl Version: %s' % CONFIG['sslVersion'])
        except ImportError:
            ssl = False
            logging.warning('import ssl module error')
            return ssl, 'Python version %s ,import ssl module error'
    print 'Testing connection to %s\t' % CONFIG['DomainName'].ljust(20)
    sys.stdout.flush()
    test_conn = None
    try:
        test_conn = obsPyCmd.MyHTTPConnection(host=CONFIG['DomainName'],
                                              is_secure=CONFIG['IsHTTPs'],
                                              ssl_version=CONFIG['sslVersion'],
                                              timeout=60)
        test_conn.create_connection()
        test_conn.connect_connection()
        ssl_ver = ''
        if CONFIG['IsHTTPs']:
            if Util.compare_version(sys.version.split()[0], '2.7.9') < 0:
                ssl_ver = test_conn.connection.sock._sslobj.cipher()[1]
            else:
                ssl_ver = test_conn.connection.sock._sslobj.version()
            rst = '\033[1;32;40mSUCCESS  %s\033[0m'.ljust(10) % ssl_ver
        else:
            rst = '\033[1;32;40mSUCCESS\033[0m'.ljust(10)
        print rst
        logging.info(
            'connect %s success, python version: %s,  ssl_ver: %s' % (
                CONFIG['DomainName'], sys.version.replace('\n', ' '), ssl_ver))
    except Exception, data:
        logging.warn('Caught exception when testing connection with %s, except: %s' % (CONFIG['DomainName'], data))
        print '\033[1;31;40m%s *%s*\033[0m' % (' Failed'.ljust(8), data)
        return False, 'Check connection failed'
    finally:
        if test_conn:
            test_conn.close_connection()

    return True, 'check passed'


def generate_run_header():
    version = '-------------------obscmd: %s, Python: %s-------------------\n' % (VERSION, sys.version.split(' ')[0])
    logging.warning(version)
    print version
    print 'Config loaded'

    return version


def prompt_for_input(config, prompt):
    item = CONFIG.get(config)
    if item is None or item == '':
        try:
            return raw_input('Please input {prompt} for {config}: '.format(prompt=prompt, config=config)).strip()
        except KeyboardInterrupt:
            print '\nTool exits...'
            sys.exit()
    else:
        return item


def run():
    version = generate_run_header()
    worker_list = []

    # Display config
    print str(CONFIG).replace('\'', '')
    logging.info(CONFIG)

    # Precondition check
    check_result, msg = precondition()
    if not check_result:
        print 'Check error, [%s] \nExit...' % msg
        sys.exit()

    msg = 'Start at %s, pid:%d. Press Ctr+C to stop. Screen Refresh Interval: 3 sec' % (
        time.strftime('%X %x %Z'), os.getpid())
    print msg
    logging.warning(msg)
    # valid_start_time: time of starting the first concurrent
    # valid_end_time: time of finishing all requests
    # current_concurrency：number of concurrent worker, -2 represents exit manually, -1 represents normally
    valid_start_time = multiprocessing.Value('d', float(sys.maxint))
    valid_end_time = multiprocessing.Value('d', float(sys.maxint))

    # Start statistic process or thread in background, for writing log, results and refresh screen print.
    results_writer = results.ResultWriter(CONFIG, TEST_CASES[CONFIG['Testcase']].split(';')[0].split(';')[0],
                                          results_queue, get_total_data_size(), valid_start_time, valid_end_time,
                                          current_concurrency)
    results_writer.daemon = True
    results_writer.name = 'resultsWriter'
    results_writer.start()
    if LOCAL_SYS != 'windows':
        print 'resultWriter started, pid: %d' % results_writer.pid
        # Make resultWriter process priority higher
        os.system('renice -19 -p ' + str(results_writer.pid) + ' >/dev/null 2>&1')
    time.sleep(.2)

    # Exit when not complete all requests
    def exit_force(signal_num, e):
        exit_msg = "\n\n\033[5;33;40m[WARN]Terminate Signal %d Received. Terminating... please wait\033[0m" % signal_num
        logging.warn('%r' % exit_msg)
        print exit_msg, '\nWaiting for all the workers exit....'
        lock.acquire()
        current_concurrency.value = -2
        lock.release()
        time.sleep(.1)
        tmp_i = 0
        for w in worker_list:
            if w.is_alive():
                if tmp_i >= 100:
                    logging.warning('force to terminate worker %s' % w.name)
                    if LOCAL_SYS != 'windows':
                        w.terminate()
                else:
                    time.sleep(.1)
                    tmp_i += 1
                    break

        print "\033[1;32;40mWorkers exited.\033[0m Waiting results_writer exit...",
        sys.stdout.flush()
        while results_writer.is_alive():
            current_concurrency.value = -2
            tmp_i += 1
            if tmp_i > 1000:
                logging.warn('retry too many times, shutdown results_writer using terminate()')
                if LOCAL_SYS != 'windows':
                    results_writer.terminate()
            time.sleep(.01)
        print "\n\033[1;33;40m[WARN] Terminated\033[0m\n"
        print version
        sys.exit()

    if LOCAL_SYS != 'windows':
        import signal

        signal.signal(signal.SIGINT, exit_force)
        signal.signal(signal.SIGTERM, exit_force)

    if CONFIG['Operation'].lower() == 'upload':
        valid_start_time.value = time.time()
        large_tuple_list = []
        small_tuple_list = []
        while not all_files_queue.empty():
            try:
                queue_tuple = all_files_queue.get(block=False)
                if queue_tuple[1] < int(CONFIG.get('MultipartObjectSize')):
                    small_tuple_list.append(queue_tuple)
                else:
                    large_tuple_list.append(queue_tuple)
            except Empty:
                continue
        for small_tuple in small_tuple_list:
            all_files_queue.put(small_tuple)
        if large_tuple_list:
            multi_parts_upload_for_list(large_tuple_list)

    # Deal with large objects first if OS is Windows.
    if LOCAL_SYS == 'windows' and CONFIG['Operation'].lower() == 'download':
        valid_start_time.value = time.time()
        large_tuple_list = []
        small_tuple_list = []

        while not all_objects_queue.empty():
            try:
                queue_tuple = all_objects_queue.get(block=False)
                if queue_tuple[1] < int(CONFIG.get('MultipartObjectSize')):
                    small_tuple_list.append(queue_tuple)
                else:
                    large_tuple_list.append(queue_tuple)
            except Empty:
                continue
        for small_tuple in small_tuple_list:
            all_objects_queue.put(small_tuple)
        if large_tuple_list:
            range_download_for_list(large_tuple_list)

    # Start concurrent business worker('Thread' in Windows, 'Process' in other OS)
    if valid_start_time.value == float(sys.maxint):
        valid_start_time.value = time.time()
    i = 0
    while i < CONFIG['Concurrency']:
        worker = Worker(target=start_worker, args=(i, CONFIG['Testcase'], valid_start_time, valid_end_time, None,
                                                   False))
        i += 1
        worker.daemon = True
        worker.name = 'worker-%d' % i
        worker.start()
        if LOCAL_SYS != 'windows':
            # Set the process priority 1 higher
            os.system('renice -1 -p ' + str(worker.pid) + ' >/dev/null 2>&1')
        worker_list.append(worker)

    logging.info('All %d workers started, valid_start_time: %.3f' % (len(worker_list), valid_start_time.value))

    time.sleep(1)
    # Exit normally
    stop_mark = False
    while not stop_mark:
        time.sleep(.3)
        if CONFIG['RunSeconds'] and (time.time() - valid_start_time.value >= CONFIG['RunSeconds']):
            logging.warning('time is up, exit')
            exit_force(99, None)
        for worker in worker_list:
            if worker.is_alive():
                break
            stop_mark = True
    for worker in worker_list:
        worker.join()
    # Wait for results_writer to finish
    logging.info('Waiting results_writer to exit...')
    while results_writer.is_alive():
        current_concurrency.value = -1  # inform results_writer
        time.sleep(.3)
    print "\n\033[1;33;40m[WARN] Terminated after all requests\033[0m\n"
    print version


if __name__ == '__main__':
    if not os.path.exists('log'):
        os.mkdir('log')
    logging.config.fileConfig('logging.conf')
    if LOCAL_SYS == 'windows':
        logging.getLogger().setLevel(logging.ERROR)
    logging.info('loading config...')

    read_config(ConfigFile.FILE_CONFIG)

    operation = CONFIG['Operation'].lower()

    # User's input
    CONFIG['BucketNameFixed'] = prompt_for_input('BucketNameFixed', 'bucket')
    bucket = CONFIG['BucketNameFixed']

    if operation == 'upload':
        # User's input
        CONFIG['LocalPath'] = prompt_for_input('LocalPath', 'directory to be uploaded')
        local_path = CONFIG['LocalPath']

        r = prompt_for_input('IgnoreExist', 'yes/no(if no, cover files already exist)')
        if CONFIG.get('IgnoreExist') is None or CONFIG.get('IgnoreExist') == '':
            if r.lower() == 'yes':
                CONFIG['IgnoreExist'] = True
            else:
                CONFIG['IgnoreExist'] = False
        is_ignore_exist = CONFIG['IgnoreExist']

        keys_already_exist = set()
        if is_ignore_exist:
            for t in get_all_keys_in_bucket(bucket_name=bucket, ak=user.ak, sk=user.sk):
                keys_already_exist.add(t[0])

        message = 'Start to scan all files in given path...' + local_path
        print message
        logging.warn(message)
        initialize_object_name(local_path, keys_already_exist)

        message = 'Number of file(s) to put: [%d]. Total size to upload is %d(B)' % (all_files_queue.qsize(),
                                                                                     total_data_upload)
        print message
        logging.warn(message)

        if all_files_queue.qsize() < 1:
            print 'Nothing to upload...exit'
            exit()

    elif operation == 'download':
        # User's input
        if CONFIG.get('DownloadTarget') is None:
            CONFIG['DownloadTarget'] = prompt_for_input('DownloadTarget',
                                                        'target in bucket to be downloaded' +
                                                        '(leave it empty for downloading entire bucket)').strip()
        download_target = CONFIG['DownloadTarget']

        # User's input
        CONFIG['SavePath'] = prompt_for_input('SavePath', 'save path')
        if not CONFIG['SavePath']:
            print 'Empty SavePath, now exit...'
            exit()

        print 'Target bucket: %s\nTarget in bucket: %s' % (bucket, download_target)

        objects = get_all_keys_in_bucket(bucket_name=bucket,
                                         ak=user.ak,
                                         sk=user.sk,
                                         target_in_bucket=download_target)
        if not objects:
            print 'Empty bucket(%s) or target(%s) not found in bucket, now exit...' % (bucket, download_target)
            exit()

        for obj in objects:
            total_data_download += obj[1]
            all_objects_queue.put(obj)
        message = '%d object(s) to be downloaded...Total size to download is %d(B)' % (len(objects),
                                                                                       total_data_download)
        print message
        logging.warn(message)

    run()
