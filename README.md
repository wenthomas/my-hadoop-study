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
