package com.wenthomas.mapreduce.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-06 20:31
 */
// 解压缩：调用CompressionCodec.createCompressionInputStream返回一个可以解压缩的输入流
// 压缩：调用CompressionCodec.createCompressionOutputStream返回一个可以压缩的输出流
public class MyCompression {

    private Path file = new Path("E:\\1015\\my_data\\mr\\compression\\test.txt");
    private Path filezip = new Path("E:\\1015\\my_data\\mr\\compression\\file.gz");

    private Configuration conf;
    private FileSystem fileSystem;

    @Before
    public void init() throws Exception{
        conf = new Configuration();
        fileSystem = FileSystem.get(conf);
    }

    /**
     * 14.使用JavaAPI完成压缩
     */
    @Test
    public void testCompression() throws ClassNotFoundException, IOException {
        //通过反射获得gzip压缩器
        String codecClassName="org.apache.hadoop.io.compress.GzipCodec";
        Class<?> codecClass = Class.forName(codecClassName);

        FSDataInputStream is = fileSystem.open(file);

        //确定使用哪种压缩格式 CompressionCodec
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        FSDataOutputStream os = fileSystem.create(new Path("e:/1015/my_data/mr/compression/file" + codec.getDefaultExtension()), true);

        //带压缩的输出流
        CompressionOutputStream outputStream = codec.createOutputStream(os);

        //流输出
        IOUtils.copyBytes(is, outputStream, conf, true);

    }

    /**
     * 15.使用JavaAPI解压缩
     * @throws IOException
     */
    @Test
    public void DeCompression() throws IOException {
        FSDataInputStream is = fileSystem.open(filezip);

        //根据后缀名获取文件对应的压缩格式
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(filezip);

        //创建一个可以解压缩的输入流
        CompressionInputStream inputStream = codec.createInputStream(is);

        //创建一个可以解压缩的输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("e:/1015/my_data/mr/compression/output.txt"), true);

        //流输出
        IOUtils.copyBytes(inputStream, outputStream, conf, true);
    }
}
