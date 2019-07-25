package com.wanghailin.ibeifeng.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	
	public static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		private Text mapOutputKey = new Text();
		private IntWritable mapOutputValue = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			//line value
			String lineValue = value.toString();
			
			//split
			StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
			
			while(stringTokenizer.hasMoreElements()){
				//set map output key
				mapOutputKey.set(stringTokenizer.nextToken());
				//output
				context.write(mapOutputKey, mapOutputValue);
			}
		}
	}
	
	public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
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
	
	public int run(String[] args){
		
		Configuration configuration = new Configuration();
		
		boolean issuccess = false;
		
		try {
			Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
			job.setJarByClass(WordCount.class);
			
			//set job
			//input
			Path inpath = new Path(args[0]);
			FileInputFormat.addInputPath(job, inpath);
			
			//output
			Path outpath = new Path(args[1]);
			FileOutputFormat.setOutputPath(job, outpath);
			
			//Mapper
			job.setMapperClass(WordCountMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			//Reducer
			job.setReducerClass(WordCountReducer.class);
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
		
		args = new String[]{
				"/user/whl/input/wordscount",
				"/user/whl/output"
		};
		
		//run job
		int status = new WordCount().run(args);
		
		System.exit(status);

	}

}
