package com.wanghailin.myself.hadoop.mapreduce.aggregation;


import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AccountPart extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		//return (key.hashCode()&Integer.MAX_VALUE)%numPartitions;
	    return new Random().nextInt(numPartitions);
	}
}
