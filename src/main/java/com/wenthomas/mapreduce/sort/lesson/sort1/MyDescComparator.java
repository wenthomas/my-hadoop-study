package com.wenthomas.mapreduce.sort.lesson.sort1;


import org.apache.hadoop.io.WritableComparator;

public class MyDescComparator extends WritableComparator{
	
	@Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      long thisValue = readLong(b1, s1);
      long thatValue = readLong(b2, s2);
      return (thisValue<thatValue ? 1 : (thisValue==thatValue ? 0 : -1));
    }

}
