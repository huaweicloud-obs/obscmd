# -*- coding:utf-8 -*-
import util
import time
import logging
import httplib
import urllib
import os
import sys
import re
import copy
import auth_handler
from urlparse import urlparse
from Queue import Full

if sys.version < '2.7':
    import myLib.myhttplib as httplib
try:
    import ssl
except ImportError:
    ssl = None
    logging.error('import ssl module error')
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    logging.warn('create unverified https context except')
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context


class MyHTTPConnection:
    def __init__(self, host, is_secure=False, ssl_version=None, timeout=80, long_connection=False, conn_header=''):
        self.is_secure = is_secure
        if self.is_secure:
            self.sslVersion = ssl.__dict__['PROTOCOL_' + ssl_version]
        self.timeout = timeout
        self.connection = None
        self.host = host
        self.root_host = self.host
        self.long_connection = long_connection
        self.conn_header = conn_header

    def create_connection(self):
        if self.is_secure:
            if util.compare_version(sys.version.split()[0], '2.7.9') >= 0:
                self.connection = httplib.HTTPSConnection(self.host + ':443', timeout=self.timeout,
                                                          context=ssl.SSLContext(self.sslVersion))
            else:
                self.connection = httplib.HTTPSConnection(self.host + ':443', timeout=self.timeout)
        else:
            self.connection = httplib.HTTPConnection(self.host + ':80', timeout=self.timeout)
        logging.debug('create connection to host: ' + self.host)

    def close_connection(self):
        if not self.connection:
            return
        try:
            self.connection.close()
        except Exception, data:
            logging.error('Caught [%s], when close a connection' % data)
            pass
        finally:
            self.connection = None

    def connect_connection(self):
        self.connection.connect()


class OBSRequestDescriptor:
    def __init__(self, request_type, ak='', sk='', auth_algorithm='', bucket="", key="", send_content='',
                 content_length=0, virtual_host=False, domain_name='obs.huawei.com', region='dftRgn'):
        self.request_type = request_type
        self.ak = ak
        self.sk = sk
        self.auth_algorithm = auth_algorithm
        self.bucket = bucket
        self.key = key
        self.send_content = send_content
        self.content_length = content_length
        self.virtual_host = virtual_host
        self.domain_name = domain_name
        self.region = region
        self.url = ''
        self.record_url = ''  # url recorded to the detail file
        self.headers = {}
        self.query_args = {}
        self.method = self._get_http_method_from_request_type_()

    def _get_http_method_from_request_type_(self):
        if self.request_type in (
                'ListUserBuckets', 'ListObjectsInBucket', 'GetObject', 'GetBucketVersioning', 'GetBucketWebsite',
                'GetBucketCORS', 'GetBucketTag', 'GetBucketLog', 'GetBucketStorageQuota', 'GetBucketAcl',
                'GetBucketPolicy',
                'GetBucketLifecycle', 'GetBucketNotification', 'GetBucketMultiPartsUpload', 'GetBucketLocation',
                'GetBucketStorageInfo', 'GetObjectUpload', 'GetObjectAcl'):
            return 'GET'
        elif self.request_type in (
                'CreateBucket', 'PutObject', 'PutBucketVersioning', 'PutBucketWebsite', 'UploadPart', 'CopyPart',
                'CopyObject', 'PutBucketCORS', 'PutBucketTag', 'PutBucketLog', 'PutBucketStorageQuota', 'PutBucketAcl',
                'PutBucketPolicy', 'PutBucketLifecycle', 'PutBucketNotification', 'PutObjectAcl'):
            return 'PUT'
        elif self.request_type in ('HeadBucket', 'HeadObject'):
            return 'HEAD'
        elif self.request_type in (
                'DeleteBucket', 'DeleteObject', 'DeleteBucketWebsite', 'DeleteBucketCORS', 'AbortMultiUpload',
                'DeleteBucketTag', 'DeleteBucketPolicy', 'DeleteBucketLifecycle'):
            return 'DELETE'
        elif self.request_type in (
                'BucketDelete', 'RestoreObject', 'DeleteMultiObjects', 'InitMultiUpload', 'CompleteMultiUpload',
                'PostObject'):
            return 'POST'
        elif self.request_type in ('OPTIONSBucket', 'OptionsObject'):
            return 'OPTIONS'
        else:
            return ''

    def generate_url(self):
        self.url = ''
        # generate url according to virtual host, bucket and key
        if self.bucket and (not self.virtual_host):
            self.url = '/%s' % self.bucket
        self.url += "/%s" % urllib.quote_plus(self.key)
        # add parameters to url
        for key in self.query_args:
            if self.query_args[key] and self.query_args[key].strip():
                if self.url.find('?') != -1:
                    self.url += ('&' + key + '=' + urllib.quote_plus(self.query_args[key]))
                else:
                    self.url += ('?' + key + '=' + urllib.quote_plus(self.query_args[key]))

            elif self.query_args[key] is None or self.query_args[key].strip() == '':
                if self.url.find('?') != -1:
                    self.url += ('&' + key)
                else:
                    self.url += ('?' + key)
        if self.bucket and self.virtual_host:  # under virtual host mode, record pattern is bucket:/key
            self.record_url = '%s:%s' % (self.bucket, self.url)
        else:
            self.record_url = self.url
        logging.debug('generate url ended, [%s]' % self.url)

    def add_content_length_header(self):
        # deal with send_content and content_length, and add to the header
        if self.send_content:
            self.content_length = self.headers['Content-Length'] = len(self.send_content)
        elif self.send_content == '' and self.content_length != 0:
            self.headers['Content-Length'] = self.content_length
        elif self.send_content == '' and self.content_length == 0:
            self.headers['Content-Length'] = 0

    def add_host_header(self, hostname=None):
        if hostname:
            self.headers['Host'] = hostname
        else:
            if not self.virtual_host:
                self.headers['Host'] = '127.0.0.1'
            elif self.bucket:
                self.headers['Host'] = self.bucket + '.' + self.domain_name
            else:
                self.headers['Host'] = self.domain_name

        logging.debug('add host header: %s' % self.headers['Host'])


class DefineResponse:
    def __init__(self):
        self.status = ''
        self.request_id = '9999999999999999'
        self.id2 = ''
        self.start_time = time.time()
        self.end_time = 0.0
        self.send_bytes = 0
        self.recv_bytes = 0
        self.return_data = None
        self.e_tag = ''
        self.recv_body = ''

    @property
    def to_string(self):
        return 'request_id: %s, status: %s,  return_data: %r, start_time: %.3f, end_time: %.3f, sendBytes: %d, ' \
               'recvBytes: %d, ETag: %s, x-amz-id-2: %s' % (self.request_id, self.status, self.return_data,
                                                            self.start_time, self.end_time, self.send_bytes,
                                                            self.recv_bytes, self.e_tag, self.id2)


class OBSRequestHandler:
    def __init__(self, obs_request, my_http_connection):
        self.obs_request = obs_request
        self.my_http_connection = my_http_connection
        self._init_connection_()
        self.my_copy_http_connection = None  # for redirecting
        # refresh url and auth header
        self.obs_request.generate_url()
        self.obs_request.add_content_length_header()
        self.obs_request.add_host_header()
        try:
            # handle auth
            if self.obs_request.auth_algorithm.lower() == 'awsv2':
                auth_handler.HmacAuthV2Handler(self.obs_request).handle()
            elif self.obs_request.auth_algorithm.lower() == 'awsv4':
                auth_handler.HmacAuthV4Handler(self.obs_request).handle()
            else:
                auth_handler.HmacAuthV2Handler(self.obs_request).handle()
        except Exception, data:
            import traceback
            stack = traceback.format_exc()
            logging.error('add authorization exception, %s\n%s' % (data, stack))

        # response object
        self.response = DefineResponse()

    def _init_connection_(self):
        # create a new connection
        if not self.my_http_connection.connection:
            if self.obs_request.bucket and self.obs_request.virtual_host:
                self.my_http_connection.host = self.obs_request.bucket + '.' + self.my_http_connection.root_host
            else:
                self.my_http_connection.host = self.my_http_connection.root_host
            self.my_http_connection.create_connection()
        # If connection exists, and the bucket changes, connection should be recreated.
        elif self.obs_request.virtual_host:
            index = self.my_http_connection.host.index(self.my_http_connection.root_host)
            if index:  # previous bucket exists
                if not self.obs_request.bucket:  # no bucket in current task
                    self.my_http_connection.host = self.my_http_connection.root_host
                    self.my_http_connection.close_connection()
                    self.my_http_connection.create_connection()
                else:  # bucket changes
                    previous_bucket = self.my_http_connection.host[0:index - 1]
                    if self.obs_request.bucket != previous_bucket:
                        self.my_http_connection.host = self.obs_request.bucket + '.' + self.my_http_connection.root_host
                        self.my_http_connection.close_connection()
                        self.my_http_connection.create_connection()
            elif self.obs_request.bucket:  # bucket doesn't exist in previous task, but appears now
                self.my_http_connection.host = self.obs_request.bucket + '.' + self.my_http_connection.root_host
                self.my_http_connection.close_connection()
                self.my_http_connection.create_connection()

        if not self.my_http_connection.conn_header:
            if self.my_http_connection.long_connection:
                self.obs_request.headers['Connection'] = 'keep-alive'
            else:
                self.obs_request.headers['Connection'] = 'close'
        else:
            self.obs_request.headers['Connection'] = self.my_http_connection.conn_header

    def _get_return_data_from_response_body_(self, body):
        if self.obs_request.request_type not in ('ListObjectsInBucket', 'InitMultiUpload', 'CopyPart', 'CopyObject'):
            return None
        if self.obs_request.request_type == 'ListObjectsInBucket':  # for marker, return None if can't find the marker
            if len(body) < 50:
                return None
            marker = re.findall('<NextMarker>.*</NextMarker>', body)
            if len(marker) > 0:
                marker = marker[0][12:-13].strip()
                if len(marker) > 0:
                    logging.debug('find next marker here %s' % marker)
                    return marker
        elif self.obs_request.request_type == 'InitMultiUpload':
            upload_id = re.findall('<UploadId>.*</UploadId>', body)
            if len(upload_id) > 0:
                upload_id = upload_id[0][10:-11].strip()
                if len(upload_id) > 0:
                    logging.debug('find upload_id here %s' % upload_id)
                    return upload_id
        elif self.obs_request.request_type == 'CopyPart' or self.obs_request.request_type == 'CopyObject':
            etag = re.findall('<ETag>.*</ETag>', body)
            if len(etag) > 0:
                etag = etag[0][6:-7].strip()
                if len(etag) > 0:
                    logging.debug('find etag here %s' % etag)
                    return etag
        logging.info('find none in body %r' % body)
        return None

    @staticmethod
    def _get_request_id_from_body_(recv_body):
        if len(recv_body) < 50:
            return ''
        request_id = re.findall('<RequestId>.*</RequestId>', recv_body)
        if len(request_id) > 0:
            request_id = request_id[0][11:-12].strip()
            if len(request_id) > 0:
                logging.debug('find request here %s' % request_id)
                return request_id
        return '9999999999999997'

    def make_request(self, is_part_upload=False, part_index=None, file_location=None, save_path_parent=None,
                     file_name=None, is_range_download=False, part_download_queue=None, range_start=None,
                     stop_flag_obj=None, is_last_retry=False):
        has_none_been_put = False
        chunk_size = 65536
        peer_addr = self.my_http_connection.host
        local_addr = ''
        http_response = None
        recv_body = ''
        self.response.start_time = time.time()
        try:
            self.my_http_connection.connection.putrequest(self.obs_request.method, self.obs_request.url, skip_host=1)
            # send headers
            for k in self.obs_request.headers.keys():
                if isinstance(self.obs_request.headers[k], list):
                    for i in self.obs_request.headers[k]:
                        self.my_http_connection.connection.putheader(k, i)
                else:
                    self.my_http_connection.connection.putheader(k, self.obs_request.headers[k])
            self.my_http_connection.connection.endheaders()
            local_addr = str(self.my_http_connection.connection.sock._sock.getsockname())
            peer_addr = str(self.my_http_connection.connection.sock._sock.getpeername())
            logging.debug('Request:[%s], conn:[%s->%s], sendURL:[%s], sendHeaders:[%r], sendContent:[%s]' % (
                self.obs_request.request_type, local_addr, peer_addr, self.obs_request.url, self.obs_request.headers,
                self.obs_request.send_content[0:1024]))

            if self.obs_request.content_length > 0 and not self.obs_request.send_content:
                if is_part_upload:
                    with open(file_location, 'rb') as obj_to_put:
                        obj_to_put.seek(part_index)
                        while self.response.send_bytes < self.obs_request.content_length:
                            if stop_flag_obj.flag:
                                raise Exception('Stop Because Some Part_upload Failed')
                            if self.obs_request.content_length - self.response.send_bytes >= chunk_size:
                                chunk = obj_to_put.read(chunk_size)
                                self.response.send_bytes += chunk_size
                            else:
                                chunk = obj_to_put.read(self.obs_request.content_length -
                                                        self.response.send_bytes)
                                self.response.send_bytes += (self.obs_request.content_length -
                                                             self.response.send_bytes)
                            self.my_http_connection.connection.send(chunk)
                else:
                    with open(file_location, 'rb') as obj_to_put:
                        while self.response.send_bytes < self.obs_request.content_length:
                            if self.obs_request.content_length - self.response.send_bytes >= chunk_size:
                                chunk = obj_to_put.read(chunk_size)
                                self.response.send_bytes += chunk_size
                            else:
                                chunk = obj_to_put.read(self.obs_request.content_length -
                                                        self.response.send_bytes)
                                self.response.send_bytes += (self.obs_request.content_length -
                                                             self.response.send_bytes)
                            self.my_http_connection.connection.send(chunk)
            else:
                self.my_http_connection.connection.send(self.obs_request.send_content)
                self.response.send_bytes += len(self.obs_request.send_content)
            wait_response_time_start = time.time()
            logging.debug('total send bytes: %d, content-length: %d' % (
                self.response.send_bytes, self.obs_request.content_length))
            # get response
            http_response = self.my_http_connection.connection.getresponse(buffering=True)
            wait_response_time = time.time() - wait_response_time_start
            logging.debug('get response, wait time %.3f' % wait_response_time)
            # read the body
            content_length = int(http_response.getheader('Content-Length', '-1'))
            logging.debug('get ContentLength: %d' % content_length)
            self.response.request_id = http_response.getheader('x-amz-request-id', '9999999999999998')
            self.response.id2 = http_response.getheader('x-amz-id-2', 'None')
            if http_response.status < 300 and self.obs_request.request_type == 'GetObject':
                if not is_range_download:
                    file_path = os.path.join(save_path_parent, file_name)
                    save_path = os.path.dirname(file_path)
                    if not os.path.isdir(save_path):
                        try:
                            os.makedirs(save_path)
                        except:
                            pass
                    with open(file_path, 'wb') as f:
                        try:
                            while True:
                                chunk = http_response.read(65536)
                                if not chunk:
                                    logging.info('chunk is empty, break cycle')
                                    recv_body = '[receive content], length: %d' % self.response.recv_bytes
                                    break
                                self.response.recv_bytes += len(chunk)
                                f.write(chunk)
                        except Exception, e:
                            logging.error('download file(%s) error(%s)' % (self.obs_request.key, e))
                            try:
                                os.remove(file_path)
                            except Exception:
                                pass
                else:
                    count = 0
                    chunk_size = 65536
                    while not stop_flag_obj.flag:
                        chunk = http_response.read(chunk_size)
                        if not chunk:
                            while not stop_flag_obj.flag:
                                try:
                                    part_download_queue.put(None, block=True, timeout=1)
                                    has_none_been_put = True
                                    logging.info('chunk is empty, break cycle')
                                    recv_body = '[receive content], length: %d' % self.response.recv_bytes
                                    break
                                except Full:
                                    pass
                            else:
                                logging.info('stop put None, range_start: %d' % range_start)
                                raise Exception('Stop Because Some Range_download Failed')
                            break
                        self.response.recv_bytes += len(chunk)
                        offset = range_start + chunk_size * count
                        data = util.Data(chunk=chunk, offset=offset)
                        while not stop_flag_obj.flag:
                            try:
                                part_download_queue.put(data, block=True, timeout=1)
                                break
                            except Full:
                                pass
                        else:
                            logging.info('stop put data, range_start: %d' % range_start)
                            raise Exception('Stop Because Some Range_download Failed')
                        count += 1
            else:
                recv_body = http_response.read()
                self.response.recv_body = recv_body
                self.response.recv_bytes = len(recv_body)
            # task end
            self.response.end_time = time.time()
            self.response.status = str(http_response.status) + ' ' + http_response.reason

            if http_response.status < 400:
                logging.debug(
                    'Request:[%s], conn: [%s->%s], URL:[%s], wait_response_time:[%.3f], responseStatus:[%s], %r, %r' % (
                        self.obs_request.request_type, local_addr, peer_addr, self.obs_request.url, wait_response_time,
                        self.response.status, str(http_response.msg), recv_body[0:1024]))
                if http_response.status in [300, 301, 302, 303, 307]:
                    # get url fron Location, redirect
                    if not http_response.getheader('location', None):
                        logging.warn('request return 3xx without header location')
                    else:
                        urlobj = urlparse(http_response.getheader('location'))
                        if not urlobj.scheme or not urlobj.hostname:
                            logging.error('location format error [%s] ' % http_response.getheader('location'))
                        else:
                            logging.debug('redirect hostname: %s, url:%s' % (urlobj.hostname, urlobj.path))
                            self.my_http_connection.close_connection()
                            self.my_copy_http_connection = copy.deepcopy(self.my_http_connection)
                            self.my_copy_http_connection.is_secure = (urlobj.scheme == 'https')
                            self.my_copy_http_connection.host = urlobj.hostname
                            self.my_copy_http_connection, self.my_http_connection = \
                                self.my_http_connection, self.my_copy_http_connection
                            self.obs_request.url = urlobj.path
                            self.obs_request.add_host_header(urlobj.hostname)
                            self.__init__(self.obs_request, self.my_http_connection)
                            logging.info(
                                'redirect the request to %s%s' % (self.my_http_connection.host, self.obs_request.url))
                            self.make_request(is_part_upload, part_index, file_location, save_path_parent,
                                              file_name, is_range_download, part_download_queue, range_start,
                                              stop_flag_obj, is_last_retry)
                            return
            elif http_response.status < 500:
                logging.warn(
                    'Request:[%s], conn: [%s->%s], URL:[%s], wait_response_time:[%.3f], responseStatus:[%s], %r, %r' % (
                        self.obs_request.request_type, local_addr, peer_addr, self.obs_request.url, wait_response_time,
                        self.response.status, str(http_response.msg), recv_body[0:1024]))
            else:
                logging.error(
                    'Request:[%s], conn: [%s->%s], URL:[%s], wait_response_time:[%.3f], responseStatus:[%s], %r, %r' % (
                        self.obs_request.request_type, local_addr, peer_addr, self.obs_request.url, wait_response_time,
                        self.response.status,
                        str(http_response.msg), recv_body[0:1024]))
                if http_response.status == 503:
                    flow_controll_msg = 'Service unavailable, local data center is busy'
                    if recv_body.find(flow_controll_msg) != -1:
                        self.response.status = '503 Flow Control'
            self.response.e_tag = http_response.getheader('ETag', 'None').strip('"')
            self.response.return_data = self._get_return_data_from_response_body_(recv_body)
            # if a result is wrong, x-amz-request-id may not be found in headers, get it in recv_body
            if self.response.request_id == '9999999999999998' and http_response.status >= 300:
                self.response.request_id = self._get_request_id_from_body_(recv_body)
            # check content length
            if self.obs_request.method != 'HEAD' and content_length != -1 \
                    and content_length != self.response.recv_bytes:
                logging.error(
                    'data error. content_length %d != recvBytes %d' % (content_length, self.response.recv_bytes))
                raise Exception("Data Error Content-Length")
        except KeyboardInterrupt:
            if not self.response.status:
                self.response.status = '9991 KeyboardInterrupt'
        except Exception, data:
            import traceback
            stack = traceback.format_exc()
            logging.error(
                'Caught exception:%s, Request:[%s], conn: [local:%s->peer:%s], URL:[%s], responseStatus:[%s], responseBody:[%r]'
                % (data, self.obs_request.request_type, local_addr, peer_addr, self.obs_request.url,
                   self.response.status,
                   recv_body[0:1024]))
            logging.error('print stack: %s' % stack)
            self.response.status = self._get_http_status_from_exception_(data, stack)
            logging.debug('self.defineResponse.status %s from except' % self.response.status)
        finally:
            # Inform to finish RangeFileWriter
            if is_range_download and not has_none_been_put and is_last_retry:
                while not stop_flag_obj.flag:
                    try:
                        part_download_queue.put(False, block=True, timeout=1)
                        stop_flag_obj.flag = True
                        break
                    except Full:
                        pass

            if self.my_copy_http_connection:
                self.my_copy_http_connection, self.my_http_connection = \
                    self.my_http_connection, self.my_copy_http_connection
            if self.response.end_time == 0.0:
                self.response.end_time = time.time()
            # close connection
            # 1. according to header connection
            if http_response and ('close' == http_response.getheader('connection', '').lower()
                                  or 'close' == http_response.getheader('Connection', '').lower()):
                logging.info('server inform to close connection')
                self.my_http_connection.close_connection()
            # 2. a connection error occurs
            elif self.response.status > '9910':
                self.my_http_connection.close_connection()
                time.sleep(.1)
            # 3. not long connection mode
            elif not self.my_http_connection.long_connection:
                self.my_http_connection.close_connection()
                # bug exists below python 2.7, status changes to CLOSE_WAIT if call close()
                if self.my_http_connection.is_secure:
                    try:
                        import sys

                        if sys.version < '2.7':
                            import gc

                            gc.collect(0)
                    except Exception, e:
                        logging.warn('make gc exception: %s' % e)

            logging.debug('finally result: %s' % self.response.to_string)
            return self.response

    @staticmethod
    def _get_http_status_from_exception_(data, stack):
        error_map = {
            'connection reset by peer': '9998',  # connection refused by server
            'broken pipe': '9997',  # pipe breaks while IO
            'timed out': '9996',  # get response timeout, config.dat's ConnectTimeout
            'badstatusline': '9995',  # wrong status code, common to that server break the connection
            'connection timed out': '9994',  # create connection timeout
            'the read operation timed out': '9993',  # read response timeout
            'cannotsendrequest': '9992',  # error on sending request
            'keyboardinterrupt': '9991',  # Ctrl+C
            'name or service not known': '9990',  # can't resolve the server's domain name
            'no route to host': '9989',  # can't reach the server's address
            'data error md5': '9901',  # check data md5 error
            'data error content-length': '9902',  # data size is different from content-length
            'stop because some range_download failed': '9801',  # other concurrent worker fails the task, stop this task
            'stop because some part_upload failed': '9802',  # other concurrent worker fails the task, stop this task
            'other error': '9999'  # other error, check log file
        }
        data = str(data).strip()
        if not data and stack:
            stack = stack.strip()
            data = stack[stack.rfind('\n') + 1:]
        if not data:
            data = 'other error'
        for (key, value) in error_map.items():
            if key in data.lower():
                return '%s %s' % (value, data)
        return '9999 %s' % data
