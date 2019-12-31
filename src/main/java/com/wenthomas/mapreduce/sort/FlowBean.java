package com.wenthomas.mapreduce.sort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Verno
 * @create 2019-12-28 16:11
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long downFlow;
    /**
     * 改写为包装类以方便调用API进行比较大小
     */
    private Long sumFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    //反序列化时，需要反射调用空参构造函数，所以必须有
    public FlowBean() {
        super();
    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    //反序列化
    //反序列化方法读顺序必须和写序列化方法的写顺序必须一致
    @Override
    public void readFields(DataInput dataInput) throws IOException {
         this.upFlow = dataInput.readLong();
         this.downFlow = dataInput.readLong();
         this.sumFlow = dataInput.readLong();
    }

    // 编写toString方法，方便后续打印到文本
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    /**
     * 重写compareTo() 方法制定新的比较规则
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {
        int result;
        //按照总流量的大小倒序比较
        if (this.sumFlow > o.getSumFlow()) {
            result = -1;
        } else if (this.sumFlow < o.getSumFlow()) {
            result = 1;
        } else {
            result = 0;
        }
        return result;
    }
}
