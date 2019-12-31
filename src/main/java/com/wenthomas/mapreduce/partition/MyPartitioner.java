package com.wenthomas.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Verno
 * @create 2019-12-31 11:21
 */
public class MyPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        //重写分区规则
        //提取手机号前三位
        String prefix = key.toString().substring(0, 3);

        int partition = 4;
        switch (prefix) {
            case "136":
                partition = 0;
                break;
            case "137":
                partition = 1;
                break;
            case "138":
                partition = 2;
                break;
            case "139":
                partition = 3;
                break;
        }
        return partition;
    }
}
