package com.wenthomas.mapreduce.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Verno
 * @create 2020-01-03 19:50
 */

/*
 * 1.输入
 * 		atguigu-a.txt\t3
 * 		atguigu-b.txt\t3
 * 		使用KeyValueTextInputFormat,可以使用一个分隔符，分隔符之前的作为key，之后的作为value
 * 2.输出
 * 		atguigu,a.txt\t3
 * 		atguigu,b.txt\t3
 */
public class MyIDMapper2 extends Mapper<Text, Text, Text, Text> {
    //使用KeyValueTextInputFormat作为输入格式，则Mapper的输入为<Text, Text>
}
