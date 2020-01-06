# my-hadoop-study
hadoop学习练习

## 一,HDFS 操作使用
### 1，HDFS客户端获取
[案例](src/main/java/com/wenthomas/hdfs/client/HdfsClient.java)
### 2，文件上传
[案例](src/main/java/com/wenthomas/hdfs/upload/MyUploadPartly.java)
### 3，文件下载
[案例](src/main/java/com/wenthomas/hdfs/upload/MyUploadPartly.java)
### 4，自定义上传下载操作
[案例](src/main/java/com/wenthomas/hdfs/download/MyDownloadPartly.java)<br/>
分块上传下载文件
### 5，文件详情操作

## 二,MapReduce编程
### 1，WordCount：MR基础编程
[案例](src/main/java/com/wenthomas/mapreduce/wordcount/)
### 2，自定义bean对象实现序列化接口（Writable）
[案例](src/main/java/com/wenthomas/mapreduce/flowcount/)
### 3，自定义InputFormat
[案例](src/main/java/com/wenthomas/mapreduce/custominputformat/)<br/>
在企业开发中，Hadoop框架自带的InputFormat类型不能满足所有应用场景，需要自定义InputFormat来解决实际问题。例如：无论HDFS还是MapReduce，在处理小文件时效率都非常低，但又难免面临处理大量小文件的场景，此时，就需要有相应解决方案。HDFS可以使用Har文件归档，MapReduce可以自定义InputFormat实现小文件的合并。
此案例中将多个输入文件按照各个文件为单位进行切片，文件名为key，文件内容为value。
### 4，自定义分区
[案例](src/main/java/com/wenthomas/mapreduce/partition/)
### 5，自定义排序
[案例](src/main/java/com/wenthomas/mapreduce/sort/)
### 6，二次排序
[案例](src/main/java/com/wenthomas/mapreduce/groupcompare/)<br/>
可以有两种实现方式：<br/>
[案例](src/main/java/com/wenthomas/mapreduce/groupcompare/MyComparator2.java)
方式1：继承WritableComparator<br/>
重写.compare(o1, o2)<br/>
[案例](src/main/java/com/wenthomas/mapreduce/groupcompare/MyComparator1.java)
方式2：实现RawComparator<br/>
重写compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 和 重写.compare(o1, o2)<br/>
### 7，自定义输出格式
[案例](src/main/java/com/wenthomas/mapreduce/outputformat/)<br/>
### 8，Reduce Join的应用
[案例](src/main/java/com/wenthomas/mapreduce/reducejoin/)<br/>
### 9，Map Join的应用
[案例](src/main/java/com/wenthomas/mapreduce/mapjoin/)<br/>
### 10，倒排索引案例（多job串联）
[案例](src/main/java/com/wenthomas/mapreduce/index/)<br/>
### 11，找博客共同好友案例
[案例](src/main/java/com/wenthomas/mapreduce/seekfriends/)<br/>
### 12，Hadoop文件压缩与解压缩
[案例](src/main/java/com/wenthomas/mapreduce/compression/)<br/>
## 三, ZooKeeper 操作使用
### 1，ZK客户端及API
[案例](src/main/java/com/wenthomas/zookeeper/test/)<br/>
### 2，监听服务器节点动态上下线案例
[案例](src/main/java/com/wenthomas/zookeeper/nodemonitor/)<br/>
