package com.wenthomas.mapreduce.groupcompare;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

import java.io.DataInput;
import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-02 18:10
 */

/*
 * 1.继承WritableCompartor  或  实现RawComparator
 *
 */
public class MyComparator1 implements RawComparator<OrderBean> {

    private OrderBean key1 = new OrderBean();
    private OrderBean key2 = new OrderBean();
    private DataInputBuffer buffer = new DataInputBuffer();


    @Override
    public int compare (byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            buffer.reset(b1, s1, l1);                   // parse key1
            key1.readFields(buffer);

            buffer.reset(b2, s2, l2);                   // parse key2
            key2.readFields(buffer);

            buffer.reset(null, 0, 0);                   // clean up reference
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return compare(key1, key2);
    }

    @Override
    public int compare(OrderBean o1, OrderBean o2) {
        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}
