package com.wenthomas.mapreduce.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Verno
 * @create 2020-01-03 17:58
 */
public class MyRJPartitioner extends Partitioner<NullWritable, MyJBean> {
    /**
     * 确保map()过来的数据按照pid分组
     * @param nullWritable
     * @param myJBean
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(NullWritable nullWritable, MyJBean myJBean, int numPartitions) {
        return (myJBean.getPid().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
