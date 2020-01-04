package com.wenthomas.mapreduce.seekfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @author Verno
 * @create 2020-01-04 8:35
 */

/*
 *keyin-valuein : (A-B,C),(A-B,E)
	reduce():
	keyout-valueout  ： (A-B:C,E)
 */
public class MySFReducer2 extends Reducer<Text, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        k.set(key);
        StringBuffer sb = new StringBuffer();
        for (Text value : values) {
            sb.append(value.toString() + ",");
        }

        v.set(sb.toString());
        context.write(k, v);
    }
}
