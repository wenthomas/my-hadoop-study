package com.wenthomas.hive.guli;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-09 9:36
 */
public class MyGuliETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text k = new Text();
    private NullWritable v = NullWritable.get();

    private StringBuilder sb = new StringBuilder();

    /**
     *  创建计数器用于统计执行结果，并根据结果判断当次数据清理是否通过
     */
    private Counter totalLines;
    private Counter passedLines;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //初始化计数器
        totalLines = context.getCounter("MyGuliETL", "TotalLines");
        passedLines = context.getCounter("MyGuliETL", "PassedLines");
    }

    /**
     * 数据清洗：
     * 1 - 将小于9列的数据过滤
     * 2 - 对大于9列的数据中的 “分类字段” 和 “相关推荐” 字段按照 “&” 进行分隔处理
     *
     * 数据示例：
     *  RX24KLBhwMI	lemonette	697	People & Blogs	512	24149	4.22	315	474	t60tW0WevkE	WZgoejVDZlo	Xa_op4MhSkg
     *  1001	Charles	3	Entertainment&People&Blogs	95	10875	4.57	661	947	1067&1188&1227&1238&1331
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1, 切割
        String[] fields = value.toString().split("\t");

        totalLines.increment(1);
        //2, 按照长度过滤不符合条件的数据
        if (fields.length >= 9) {
            //3, 第四列 “所属分类” 字段 替换分隔符为 "&"
            fields[3] = fields[3].replace(" ", "");

            //4, 第10列及以后 “相关推荐” 字段 替换分隔符为 "&"
            // 每次map()都清空sb中内容
            if (sb.length() != 0) {
                sb.setLength(0);
            }

            for (int i = 0; i <fields.length; i++) {
                if (i < 9) {
                    sb.append(fields[i]).append("\t");
                } else if (i == fields.length - 1) {
                    sb.append(fields[i]);
                } else {
                    sb.append(fields[i]).append("&");
                }
            }

            k.set(sb.toString());

            passedLines.increment(1);

            context.write(k, v);
        }
    }
}
