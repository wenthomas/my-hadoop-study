package com.wenthomas.mapreduce.seekfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-03 20:56
 */

/*
 * keyin-valuein:  （A:B,C,D,F,E,O）
	map(): 将valuein拆分为若干好友，作为keyout写出
			将keyin作为valueout
	keyout-valueout: （友:用户）
					（c:A）,(C:B),(C:E)
 */
public class MySFMapper1 extends Mapper<Text, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split(",");
        for (String string : strings) {
            k.set(string);
            v.set(key);
            context.write(k, v);
        }
    }
}
