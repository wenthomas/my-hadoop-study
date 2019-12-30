package com.wenthomas.mapreduce.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Verno
 * @create 2019-12-28 17:32
 */
public class MyReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
            throws IOException, InterruptedException {
/*        Iterator var4 = values.iterator();

        while(var4.hasNext()) {
            VALUEIN value = var4.next();
            context.write(key, value);
        }*/
        long sumUpFlow = 0;
        long sumDownFlow = 0;

        for (FlowBean value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }

        v.setUpFlow(sumUpFlow);
        v.setDownFlow(sumDownFlow);
        v.setSumFlow(sumUpFlow + sumDownFlow);

        context.write(key, v);
    }
}
