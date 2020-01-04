package com.wenthomas.mapreduce.seekfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Verno
 * @create 2020-01-03 21:41
 */

/*
keyin-valuein:   （友\t用户，用户，用户，用户）
	map()：  使用keyin作为valueout
				将valuein切分后，两两拼接，作为keyout
	keyout-valueout: （用户-用户，友）
					(A-B,C),(A-B,E)
					  (A-E,C), (A-G,C), (A-F,C), (A-K,C)
					  (B-E,C),(B-G,C)
 */
public class MySFMapper2 extends Mapper<Text, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] users = value.toString().split(",");
        //保证数组中的用户名有序
        Arrays.sort(users);

        v.set(key);
        //将valuein切分后，两两拼接作为keyout
        for (int i = 0; i <= users.length - 1; i++) {
            for (int j = i + 1; j < users.length ; j++) {
                k.set(users[i] + "-" + users[j]);
                context.write(k, v);
            }
        }
    }
}
