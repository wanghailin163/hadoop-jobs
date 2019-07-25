package com.wanghailin.myself.hadoop.mapreduce.aggregation;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AccountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private IntWritable reduceputvalue = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		int count = 0;
		
		for(IntWritable value:values){
			count+=value.get();
		}
		
		reduceputvalue.set(count);
		
		context.write(key,reduceputvalue);
		
	}
}
