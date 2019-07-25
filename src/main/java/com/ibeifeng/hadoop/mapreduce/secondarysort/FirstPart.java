package com.ibeifeng.hadoop.mapreduce.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPart extends Partitioner<PairWritable, IntWritable> {

	@Override
	public int getPartition(PairWritable key, IntWritable value,
			int numPartitions) {
		return (key.getFirst().hashCode()&Integer.MAX_VALUE)%numPartitions;
	}

}
