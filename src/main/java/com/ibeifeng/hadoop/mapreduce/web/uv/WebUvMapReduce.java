package com.wanghailin.ibeifeng.hadoop.mapreduce.web.uv;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WebUvMapReduce extends Configured implements Tool{
	
	
	
	public static class WebUvMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
		
		private Text mapoutputkey = new Text();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			//line value
			String lineValue = value.toString();
			
			
			//split
			String[] str = lineValue.split("\t");
			
			if(str.length<30){
				return;
			}
			
			//get id
			String uid = str[5];
			if(StringUtils.isBlank(uid)){
				return;
			}
			
			//time
			String time = str[17];
			if(StringUtils.isBlank(time)){
				return;
			}
			
			//subtime
			String date = time.substring(0, 10);
			
			//pid
			String pid = str[23];
			if(StringUtils.isBlank(pid)){
				return;
			}
			try {
				Integer.valueOf(pid);
			} catch (Exception e) {
				return;
			}
		    
		    //date_pid_uid
			mapoutputkey.set(date+"\t"+pid+"_"+uid);
			
			//output
		    context.write(mapoutputkey,NullWritable.get());
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
		}
	}
	
	/*
	public static class DatajoinCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
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
	*/
	
	public static class WebUvReducer extends Reducer<Text,NullWritable,Text,IntWritable>{
		//key
		private Text outputkey = new Text();
		//map
		private Map<String,Integer> dateMap;
		//value
		private IntWritable outputvalue = new IntWritable();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			dateMap = new HashMap<String,Integer>();
		}
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,Context context)
				throws IOException, InterruptedException {
			//get date /t pid  20170118\t29
			String datepid = key.toString().split("_")[0];
			if(!dateMap.containsKey(datepid)){
				dateMap.put(datepid, 1);
			}else{
				Integer sum = dateMap.get(datepid);
				sum = sum + 1;
				dateMap.put(datepid, sum);
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Set<String> datemapset = dateMap.keySet();
			for(String datepid : datemapset){
				outputkey.set(datepid);
				outputvalue.set(dateMap.get(datepid));
				
				context.write(outputkey, outputvalue);
			}
		}
	}
	
	public int run(String[] args){
		
		Configuration configuration = this.getConf();
		
		boolean issuccess = false;
		
		try {
			Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
			job.setJarByClass(WebUvMapReduce.class);
			
			//set job
			//input
			Path inpath = new Path(args[0]);
			FileInputFormat.addInputPath(job, inpath);
			
			//output
			Path outpath = new Path(args[1]);
			FileOutputFormat.setOutputPath(job, outpath);
			
			//Mapper
			job.setMapperClass(WebUvMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			//  =============shuffle============
			//job.setPartitionerClass(cls);
			//job.setSortComparatorClass(cls);
			//job.setGroupingComparatorClass(cls);
			//job.setCombinerClass(WordSortCombiner.class);
			
			//Reducer
			job.setReducerClass(WebUvReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setNumReduceTasks(7);//设置reduce数量
		
			
			//submit job -> yarn
			issuccess = job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return issuccess ? 0 : 1;
	}

	public static void main(String[] args) {
		
		Configuration configuration = new Configuration();
		
		/*args = new String[]{
				"/user/whl/input/2015082819",
				"/user/whl/output9"
		};*/
		
		//run job
		try {
			int status = ToolRunner.run(configuration, new WebUvMapReduce(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
