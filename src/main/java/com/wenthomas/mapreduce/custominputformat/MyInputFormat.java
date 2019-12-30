package com.wenthomas.mapreduce.custominputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 * @author Verno
 * @create 2019-12-30 18:37
 */

/*
 * 1. 改变切片策略，一个文件固定切1片，通过指定文件不可切
 *
 * 2. 提供RR ，这个RR读取切片的文件名作为key,读取切片的内容封装到bytes作为value
 */
public class MyInputFormat extends FileInputFormat {
    /**
     * 输入文件是否可分割：false-不可分割，整个文件作为一个切片
     * @param context
     * @param filename
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }


    /**
     * 需要创建自定义RecordReader类来实现自定义切片下mapper读入k-v的形式。
     * @param split
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MyCustomRecordReader();
    }
}
