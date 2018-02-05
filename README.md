# obscmd
This tool is a client tool for Huawei, OBS running in shell or cmd, for processing data download and upload.


-- UPDATES:
2018.1.31 - v4.5.0
1. Added - add a configuration for checking the file is changing before putting object.
2. Fixed - if comparison of etag and md5 fails, delete the object.

2018.1.23 - v4.4.0
1. Added - enhance the timed task tool

2018.1.20 - v4.3.0
1. Fixed - normalize the variables' name
2. Fixed - cancel windows support, cancel dealing with large files file first in upload.

2018.1.17 - v4.2.1
1. Fixed - optimize the format of brief file
2. Fixed - some spelling errors

2018.1.16 - v4.2.0
1. Fixed - use set() to compare remote with local instead of list()
2. Fixed - change the interpreter header to '#!/usr/bin/env python' in the run file

2018.1.12 - v4.1.0
1. Fixed - not work with virtual host mode.

2018.1.11 - v4.0.0
1. Add feature - Record ETag to detail file and retry on status and comparing ETag for put_object.
2. Add feature - Record x-amz-id-2 to detail file.
3. Fixed - progress calculated from the requests number to the data size.

2018.1.4 - v3.7.5
1. Add feature - Specify multiple files for uploading.

2018.1.2 - v3.7.4
1. Fixed setting of headers of content-type.

2017.12.28 - v3.7.3
1. Add feature - Specify multiple objects or prefix or mixed for downloading.

2017.12.15 - v3.7.2
1. fix a bug for net speed refreshing.

2017.12.11 - v3.7.1
1. Fix bugs for wrong comments.
2. Catch the exception for listing local dirs.
3. Deal with large files uploading first under any situation.

2017.12.06 - v3.7.0
1. Change the strategy of default authorization header algorithm.
2. In linux, when upload, deals with large or small files randomly.(Deal with large files first before)
3. Fix a bug for the progress bar display.
4. Add protection for the upload part size[5MB, 5GB].

2017.11.30 - v3.6.1
1. Fix bug for requests count refresh in failed multipart upload.

2017.11.30 - v3.6.0
1. Retry three times, the interval is 5s, and bug fixed.
2. Optimize screen print.

2017.11.28 - v3.5.0
1. Retry once in multipart tasks.
2. Optimize exceptions in multipart tasks.

2017.11.25 - v3.2.0
Create ReadMe
Initial features：
1.  Upload multiple files concurrently.
2.  Multipart upload for large files.
3.  Chose to overwrite objects that already in the bucket.
4.  Download multiple objects concurrently.
5.  Multipart download large objects by range download concurrently.
6.  Timed task by running a script.
7.  Support Windows(but deal with large objects/files first)
8.  Protection for multipart upload - can't be more than 10,000 parts.
9.  Upload to a specific directory in a bucket.
10. Download a single object, download a specific directory in a bucket and download the entire bucket.

how to use:

step 1: install python,the version is more than 2.7.9;

  apt install gcc   // ubuntu 操作系统
  
  yum install gcc   //RedHat ，CentOs类操作系统
  
wget https://www.python.org/ftp/python/2.7.14/Python-2.7.14.tgz 

tar -xvf Python-2.7.14.tgz    //解压到本地

cd Python-2.7.14             //进入解压后的目录 

./configure                  //进行配置

make   // 进行编译

make  install // 进行安装

rm /usr/bin/python  //删除之前版本链接 

ln -s /usr/local/bin/python2.7 /usr/bin/python   //创建新版本python链接 

python -v   //查看 python版本是不是已经变为安装的版本

step 2: install openssl, the version is morthan 1.0.2n;

wget https://www.openssl.org/source/openssl-1.0.2n.tar.gz

 tar -xvf  openssl-1.0.2n.tar.gz    // 解压到当前目录
 
cd openssl-1.0.2n    // 进入解压后的目录

./config -fPIC  //  进行配置

make && make install

rm /usr/bin/opensll  // 删除之前的openssl PATH变量 

ln -s /usr/local/ssl/bin/openssl /usr/bin/opensll  //创建新的openssl版本路径;

step3.
vi config.dat input the neccessnary data;

public config：

AK	//Access Key ID 接入键标识

SK	//Secret Access Key安全接入键

MultipartObjectSize	 //进行分段操作的对象大小分界值（Byte）

PartSize	 //段大小（Byte）

Concurrency	//并发数

BucketNameFixed	//桶名

Region	//区域

VirtualHost	//是否使用虚拟主机方式请求的开关

DomainName	//域名

IsHTTPs	//Https开关

sslVersion	//ssl协议版本

upload config：

LocalPath	  //待上传目录

IgnoreExist	 //忽略上传到桶内的文件对象的开关

RemoteDir	上//传到桶内的目录

PutWithACL	//Access Control Policy 对象的权限控制策略

download config：

DownloadTarget	//待下载的OBS桶内目标

SavePath	//本地保存路径

step4.
 pythone run.py or ./run.py  to do the task
 
step5.
  go to dir results and see the detail task process results.
 
