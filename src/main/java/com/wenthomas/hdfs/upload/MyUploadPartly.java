package com.wenthomas.hdfs.upload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Verno
 * @create 2019-12-25 20:48
 */
public class MyUploadPartly {

    Configuration configuration;

    FileSystem fileSystem;

    /**
     * 前置操作： 获取hdfs客户端
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Before
    public void loadHDFSClient() throws URISyntaxException, IOException, InterruptedException {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "atguigu");
    }

    /**
     * 后置操作：释放资源
     * @throws IOException
     */
    @After
    public void closeHDFSClient() throws IOException {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }
    /**
     * 自定义hdfs上传文件：
     */
    @Test
    public void putFileToHDFS() throws IOException {
        File input = new File("E:\\1015\\my_data\\mr\\hello.txt");
        Path output = new Path("/mr/chapter01/hello.txt");
        //1, 获取hdfs客户端
        //FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "atguigu");
        //2, 创建输入流（本地）
        FileInputStream fileInputStream = new FileInputStream(input);
        //3, 获取输出流（hdfs文件系统）
        FSDataOutputStream fos = fileSystem.create(output);
        //4, 数据传输
        IOUtils.copyBytes(fileInputStream, fos, configuration);
        //5, 释放资源
        fos.close();
        fileInputStream.close();
    }

    /**
     * 拆分下载：仅下载文件的前128M部分
     * @throws IOException
     */
    //todo: 编写文件按照指定大小分段下载
    @Test
    public void downloadFilePartly() throws IOException {
        Path input = new Path("/hdfs/eclipse.zip");
        File output = new File("E:\\1015\\my_data\\hdfs\\eclipse.zip.part01");
        //1, 获取hdfs客户端
        //loadHDFSClient();

        //2, 获取输入流（hdfs文件系统）
        FSDataInputStream fis = fileSystem.open(input);
        //3, 创建输出流（本地）
        FileOutputStream fos = new FileOutputStream(output);

        //4, 流的拷贝
        byte[] buffer = new byte[1024];
        for(int i = 0; i < 1024 * 128; i++) {
            fis.read(buffer);
            fos.write(buffer);
        }

        //5, 释放资源
        fos.close();
        fis.close();
    }
}
