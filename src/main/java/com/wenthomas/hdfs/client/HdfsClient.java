package com.wenthomas.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Verno
 * @create 2019-12-25 18:44
 */
public class HdfsClient {
    /**
     * 获取及操作hdfs集群案例
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        //1,获取文件系统客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "atguigu");
        //2,创建目录
        fileSystem.mkdirs(new Path("/hdfs/study"));
        //3,释放资源
        fileSystem.close();
    }
}
