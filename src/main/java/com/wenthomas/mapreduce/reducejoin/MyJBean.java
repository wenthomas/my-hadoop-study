package com.wenthomas.mapreduce.reducejoin;

import com.sun.org.apache.bcel.internal.generic.DADD;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Verno
 * @create 2020-01-03 15:49
 */
public class MyJBean implements Writable {

    private String orderId;
    private String pid;
    private String pname;
    private String amount;
    private String source;

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

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public String toString() {
        return  orderId + "\t" +  pname + "\t" + amount ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(pid);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(amount);
        dataOutput.writeUTF(source);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.pname = dataInput.readUTF();
        this.amount = dataInput.readUTF();
        this.source = dataInput.readUTF();
    }
}
