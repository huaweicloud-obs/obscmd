# -*- coding:utf-8 -*-
import base64
import threading
import logging
import subprocess
import os
import errno
import hashlib

from constant import IS_PYTHON2

TIME_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'
ISO8601 = '%Y%m%dT%H%M%SZ'
ISO8601_MS = '%Y-%m-%dT%H:%M:%S.%fZ'
RFC1123 = '%a, %d %b %Y %H:%M:%S %Z'


def get_utf8_value(value):
    if not value:
        return ''
    if isinstance(value, str):
        return value
    if isinstance(value, unicode):
        return value.encode('utf-8')
    return str(value)


def compare_version(v1, v2):
    v1 = v1.split('.')
    v2 = v2.split('.')
    try:
        for i in range(0, len(v1)):
            if len(v2) < i + 1:
                return 1
            elif int(v1[i]) < int(v2[i]):
                return -1
            elif int(v1[i]) > int(v2[i]):
                return 1
    except Exception:
        return -1
    if len(v2) > len(v1):
        return -1
    return 0


def convert_time_format_str(time_sec):
    if time_sec < 0:
        return '--\'--\'--'
    if time_sec >= 8553600:
        return '>99 days'
    elif time_sec >= 86400:
        return '%2.2d Days %2.2d\'%2.2d\'%2.2d' % (
            time_sec / (3600 * 24), time_sec % (3600 * 24) / 3600,
            (time_sec % 3600 / 60), (time_sec % 60))
    else:
        ms = time_sec - int('%2.2d' % (time_sec % 60))
        return '%2.2d\'%2.2d\'%2.2d.%d' % (
            time_sec / 3600, (time_sec % 3600 / 60), (time_sec % 60), ms * 1000)


def generate_response(response):
    """
    response of server always contains "\r\n", need to remove it
    :param response: response of server
    :return:
    """
    if response is not None:
        resp = response.split('\r\n')
        resp = resp[0]
        return resp
    else:
        raise Exception("response of server is none, please confirm it.")

def rename(src, dst):
    subprocess.Popen("mv '{src}' '{dst}'".format(src=src, dst=dst),
                     shell=True).communicate()


def delete_file(file_path):
    if os.path.isfile(file_path):
        try:
            os.remove(file_path)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def calculate_file_md5(file_location, part_start=0, part_size=None):
    m = hashlib.md5()
    with open(file_location, 'rb') as f:
        size_to_hash = part_size if part_size else (
            int(os.path.getsize(file_location)) - part_start)
        chunk_size = 65536
        f.seek(part_start)
        while size_to_hash:
            if size_to_hash > chunk_size:
                data = f.read(chunk_size)
                size_to_hash -= chunk_size
            else:
                data = f.read(size_to_hash)
                size_to_hash = 0
            m.update(data)
    return m.hexdigest()



# 功能 ： 对文件进行MD5计算
# file_path ： 文件路径
# size ：要加密的内容大小
# offset ： 指针偏移量
# return ： MD5值，需要以进制数处理 ；
#           m.digest()以二进制数输出，一般与base64.b64encode()进行再次加密
#           m.hexdigest()以16进制数输出
def md5_file_encode_by_size_offset(file_path=None, size=None, offset=None,
                                   chuckSize=None):
    if file_path is not None and size is not None and offset is not None:
        m = hashlib.md5()
        with open(file_path, 'rb') as fp:
            CHUNKSIZE = 65536 if chuckSize is None else chuckSize
            fp.seek(offset)
            read_count = 0
            while read_count < size:
                read_size = CHUNKSIZE if size - read_count >= CHUNKSIZE else size - read_count
                data = fp.read(read_size)
                read_count_once = len(data)
                if read_count_once <= 0:
                    break
                m.update(data)
                read_count += read_count_once
        return m


class User:
    def __init__(self, username, ak, sk):
        self.username = username
        self.ak = ak
        self.sk = sk


class Counter:
    def __init__(self):
        self.count = 0


class ThreadsStopFlag:
    def __init__(self):
        self.flag = False


class Data:
    def __init__(self, chunk, offset):
        self.chunk = chunk
        self.offset = offset


class RangeFileWriter(threading.Thread):
    def __init__(self, queue, file_path, parts_count):
        threading.Thread.__init__(self)
        self.queue = queue
        self.file_path = file_path
        self.parts_count = parts_count

    def run(self):
        temp_file_path = self.file_path + '.downloading'
        count = 0
        should_delete = False
        with open(temp_file_path, 'wb') as f:
            logging.info(
                'start writing range file(' + temp_file_path + '), parts_count=' + str(
                    self.parts_count))
            while True:
                data = self.queue.get(block=True)
                if data is None:
                    # A None is a end sign for completing a part downloading.
                    count += 1
                    logging.info(
                        'range file(' + temp_file_path + ') receives ' + str(
                            count) + ' None(s)')
                    if count == self.parts_count:
                        break
                    continue
                elif data is False:
                    # A False is a sign for failure of a part downloading.
                    logging.error(
                        'file: %s, some part_download task failed, delete it.' % self.file_path)
                    should_delete = True
                    break
                f.seek(data.offset)
                f.write(data.chunk)
                f.flush()
        if should_delete:
            try:
                os.remove(temp_file_path)
            except OSError:
                pass
        else:
            rename(temp_file_path, self.file_path)
            logging.info('complete rename range file(' + self.file_path + ')')

def base64_encode(unencoded):
    unencoded = unencoded if IS_PYTHON2 else (
    unencoded.encode('UTF-8') if not isinstance(unencoded,
                                                bytes) else unencoded)
    encode_str = base64.b64encode(unencoded, altchars=None)
    return encode_str if IS_PYTHON2 else encode_str.decode('UTF-8')
