package com.wenthomas.mapreduce.groupcompare;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Verno
 * @create 2020-01-02 18:48
 */
/*
 * 1.继承WritableCompartor  或  实现RawComparator
 *
 */
public class MyComparator2 extends WritableComparator {

    public MyComparator2() {
        super(OrderBean.class, null, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean o1=(OrderBean) a;
        OrderBean o2=(OrderBean) b;
        return o1.getOrderId().compareTo(o2.getOrderId());
    }

}
