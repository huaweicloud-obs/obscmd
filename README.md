# obscmd
This tool is a client for Huawei OBS running in shell or cmd, for processing data download and upload.
Please refer to the word doc for configuration and start instruction.


-- UPDATES:
2018.1.17 - v4.2.1

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
ThreadsPerUser	//并发数
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
 
