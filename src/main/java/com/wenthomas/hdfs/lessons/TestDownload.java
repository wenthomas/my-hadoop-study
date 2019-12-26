package com.wenthomas.hdfs.lessons;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Verno
 * @create 2019-12-26 15:50
 */
public class TestDownload {
    /**
     * 下载文件第一部分内容
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "atguigu");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hdfs/eclipse.zip"));

        fis.seek(1*1024*1024*128);
        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("E:\\1015\\my_data\\hdfs\\eclipse.zip.021"));

        // 4 流的拷贝
        byte[] buf = new byte[1024];

        for(int i =0 ; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }

        // 5关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }


    /**
     * 下载文件第二部分内容
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "atguigu");

        // 2 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hdfs/eclipse.zip"));

        // 3 定位输入数据位置
        fis.seek(1*1024*1024*128);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("E:\\1015\\my_data\\hdfs\\eclipse.zip.021"));

        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

}
