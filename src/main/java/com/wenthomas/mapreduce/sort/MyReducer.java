package com.wenthomas.mapreduce.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Verno
 * @create 2019-12-28 17:32
 */
public class MyReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    private FlowBean v = new FlowBean();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // 循环输出，避免总流量相同情况
        for (Text value : values) {
            context.write(value, key);
        }

    }
}
