package com.wenthomas.mapreduce.mapjoin.lesson;


public class JoinBean {
	
	private String orderId;
	private String pid;
	private String pname;
	private String amount;
	
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


}
