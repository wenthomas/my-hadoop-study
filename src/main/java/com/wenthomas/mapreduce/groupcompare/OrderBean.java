package com.wenthomas.mapreduce.groupcompare;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-02 18:00
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;

    private String pid;

    private Double amount;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    // 二次排序，先按照orderid排序(升降序都可以)，再按照acount(降序)排序
    @Override
    public int compareTo(OrderBean o) {
        //先按照orderid排序升序排序
        int result = this.orderId.compareTo(o.getOrderId());

        if (result == 0) {
            //再按照acount(降序)排序
            result = -this.amount.compareTo(o.getAmount());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(pid);
        dataOutput.writeDouble(amount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readUTF();
        pid = dataInput.readUTF();
        amount = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + pid + "\t" + amount ;
    }
}
