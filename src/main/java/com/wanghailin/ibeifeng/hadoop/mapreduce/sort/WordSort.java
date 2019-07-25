package com.wanghailin.ibeifeng.hadoop.mapreduce.sort;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordSort extends Configured implements Tool{
	
	public static class WordSortMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		private Text mapOutputKey = new Text();
		private IntWritable mapOutputValue = new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			//line value
			String lineValue = value.toString();
			
			//split
			String[] str = lineValue.split(",");
			
			if(str.length!=2){
				return;
			}
			
		    mapOutputKey.set(str[0]);
		    mapOutputValue.set(Integer.parseInt(str[1]));
		    
		    context.write(mapOutputKey,mapOutputValue);
		}
	}
	
	public static class WordSortCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable outputValue = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			//temp sum
			int sum = 0;
			//iterator
			for(IntWritable value : values){
				sum += value.get();
			}
			//set output
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}
	
	public static class WordSortReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable outputValue = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			/*//temp sum
			int sum = 0;
			//iterator
			for(IntWritable value : values){
				sum += value.get();
			}
			//set output
			outputValue.set(sum);
			context.write(key, outputValue);*/
			
			for(IntWritable value : values){
				context.write(key, value);
			}
		}
	}
	
	public int run(String[] args){
		
		Configuration configuration = this.getConf();
		
		boolean issuccess = false;
		
		try {
			Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
			job.setJarByClass(WordSort.class);
			
			//set job
			//input
			Path inpath = new Path(args[0]);
			FileInputFormat.addInputPath(job, inpath);
			
			//output
			Path outpath = new Path(args[1]);
			FileOutputFormat.setOutputPath(job, outpath);
			
			//Mapper
			job.setMapperClass(WordSortMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			//  =============shuffle============
			//job.setPartitionerClass(cls);
			//job.setSortComparatorClass(cls);
			//job.setGroupingComparatorClass(cls);
			//job.setCombinerClass(WordSortCombiner.class);
			
			//Reducer
			job.setReducerClass(WordSortReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			//submit job -> yarn
			issuccess = job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return issuccess ? 0 : 1;
	}

	public static void main(String[] args) {
		
		Configuration configuration = new Configuration();
		
		args = new String[]{
				"/user/whl/input/wordsort",
				"/user/whl/output5"
		};
		
		//run job
		try {
			int status = ToolRunner.run(configuration, new WordSort(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
	
	}

}
