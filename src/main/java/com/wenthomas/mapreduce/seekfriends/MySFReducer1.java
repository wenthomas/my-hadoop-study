package com.wenthomas.mapreduce.seekfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-03 21:19
 */

/*
 * keyin-valuein : （友:用户）
					（c:A）,(C:B),(C:E)
	reduce():
	keyout-valueout  ：（友：用户，用户，用户，用户）
 */
public class MySFReducer1 extends Reducer<Text, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();

        for (Text value : values) {
            //sb.append(value.toString()).append(",");
            sb.append(value.toString() + ",");
        }
        k.set(key);
        v.set(sb.toString());
        context.write(k, v);
    }
}
