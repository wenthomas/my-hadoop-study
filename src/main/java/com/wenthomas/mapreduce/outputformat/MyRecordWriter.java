package com.wenthomas.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.net.URI;

/**
 * @author Verno
 * @create 2020-01-03 11:47
 */
public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

    private String atguiguPath = "E:\\1015\\my_data\\mr\\output\\atguigu.log";
    private String otherPath = "E:\\1015\\my_data\\mr\\output\\otherPath.log";
    private Configuration configuration;
    private FileSystem fs;
    private FSDataOutputStream atguiguFS;
    private FSDataOutputStream otherFS;

    public MyRecordWriter(TaskAttemptContext job) {
        configuration = job.getConfiguration();
        try {
            fs = FileSystem.get(configuration);
            atguiguFS = fs.create(new Path(atguiguPath));
            otherFS = fs.create(new Path(otherPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //根据域名分类输出到不同的文件中去
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String url = key.toString() + "\n";

        if (url.contains("atguigu")) {
            atguiguFS.write(url.getBytes());
        } else {
            otherFS.write(url.getBytes());
        }
    }

    //释放资源
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (null != atguiguFS) {
            atguiguFS.close();
        }
        if (null != otherFS) {
            otherFS.close();
        }
        if (null != fs) {
            fs.close();
        }
    }
}
