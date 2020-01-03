package com.wenthomas.mapreduce.reducejoin.lesson;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class JoinBean implements Writable{
	
	private String orderId;
	private String pid;
	private String pname;
	private String amount;
	private String source;
	
	

	@Override
	public String toString() {
		return  orderId + "\t" +  pname + "\t" + amount ;
	}

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
	public void write(DataOutput out) throws IOException {
		out.writeUTF(orderId);
		out.writeUTF(pid);
		out.writeUTF(pname);
		out.writeUTF(amount);
		out.writeUTF(source);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		orderId=in.readUTF();
		pid=in.readUTF();
		pname=in.readUTF();
		amount=in.readUTF();
		source=in.readUTF();
		
	}

}
