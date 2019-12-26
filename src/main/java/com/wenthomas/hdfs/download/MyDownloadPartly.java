package com.wenthomas.hdfs.download;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Verno
 * @create 2019-12-26 14:20
 */
public class MyDownloadPartly {

    private final static Logger log = LoggerFactory.getLogger(MyDownloadPartly.class);

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
     * 按照指定大小分块下载文件
     * 定位内容: .seek()方法
     * @throws IOException
     */
    @Test
    public void DownloadPartly() throws Exception {
        //指定下载文件块大小：128M
        int blockSize = 128;

        Path input = new Path("/hdfs/eclipse.zip");

        String output = "E:\\1015\\my_data\\hdfs\\eclipse.zip";

        //1, 获取hdfs客户端
        //2, 获取输入流（hdfs)
        FSDataInputStream fis = fileSystem.open(input);
        //3, 创建输出流（本地）
        FileOutputStream fos;
        //4, 流的拷贝
        //---------查看目标文件大小
        //todo: 校验是否文件夹
        if (!fileSystem.exists(input)) {
            throw new Exception("不存在该目标文件");
        }
        long size = fileSystem.getFileStatus(input).getLen();

        //---------文件块
        int part = 1;
        //---------每个缓冲块大小：1024
        byte[] buffer = new byte[1024];

        while (size > 0) {
            //---------文件块定位读取
            fis.seek(Long.parseLong(Integer.toString((part - 1) * 128 * 1024 * 1024)));
            fos = new FileOutputStream(new File(output + ".0" + part));
            if (size > 0) {
                if (size >= 128 * 1024 * 1024) {
                    for(int i = 1; i < 1024 * 128; i++) {
                        fis.read(buffer);
                        fos.write(buffer);
                    }
                } else {
                    IOUtils.copyBytes(fis, fos, configuration);
                    IOUtils.closeStream(fos);
                    IOUtils.closeStream(fis);
                }
            }
            log.info("第" + part + "块文件块下载完成！" );
            part++;
            size -= 128 * 1024 *1024;

            if (null != fos) {
                fos.close();
            }
        }
        log.info("文件全部下载完成！");
        //5, 释放资源
        if (null != fis) {
            fis.close();
        }
    }
}
