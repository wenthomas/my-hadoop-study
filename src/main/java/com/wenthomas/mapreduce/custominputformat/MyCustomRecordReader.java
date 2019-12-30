package com.wenthomas.mapreduce.custominputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ID;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author Verno
 * @create 2019-12-30 19:10
 */

/*
 * RecordReader从MapTask处理的当前切片中读取数据
 *
 * XXXContext都是Job的上下文，通过XXXContext可以获取Job的配置Configuration对象
 */
public class MyCustomRecordReader extends RecordReader {

    /**
     * Mapper读入的key为输入文件的文件名
     */
    private Text key;

    /**
     * Mapper读入的value为输入文件的内容，这里以二进制数组来保存文件的内容
     */
    private BytesWritable value;

    private String fileName;
    private int length;

    private Configuration configuration;
    private FileSystem fileSystem;
    private FSDataInputStream fis;

    private boolean flag=true;

    /**
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //强转为FileSplit以获取文件信息: 文件名（作为Mapper的Key）+ 文件长度（用以切割文件）
        FileSplit fileSplit = (FileSplit) split;
        fileName = fileSplit.getPath().getName();
        length = Integer.valueOf(String.valueOf(fileSplit.getLength()));

        configuration = context.getConfiguration();
        fileSystem = FileSystem.get(configuration);

        fis = fileSystem.open(fileSplit.getPath());
    }

    /**
     * 方法返回true时，Mapper会调用map()读取此方法输出的k-v
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (flag) {
            //实例化对象
            if (key==null) {
                key=new Text();
            }

            if (value==null) {
                value=new BytesWritable();
            }

            //设置mapper读入的key值
            key.set(fileName);

            byte[] buffer = new byte[length];
            IOUtils.readFully(fis, buffer, 0, length);
            //设置mapper读入的value值
            value.set(buffer, 0, length);

            flag = false;

            return true;
        }

        return false;
    }

    //返回当前读取到的key-value中的key
    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    //返回当前读取到的key-value中的value
    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    //返回读取切片的进度（可选）
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    // 在Mapper的输入关闭时调用，清理工作
    @Override
    public void close() throws IOException {
        if (null != fis) {
            fis.close();
        }
        if (null != fileSystem) {
            fileSystem.close();
        }
    }
}
