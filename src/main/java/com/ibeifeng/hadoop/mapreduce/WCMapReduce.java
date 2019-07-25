package com.ibeifeng.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WCMapReduce extends Configured implements Tool{
	
	public static Logger log = LoggerFactory.getLogger(WCMapReduce.class);
	
	public static class WCMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
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
	
	public static class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
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
		
		Configuration configuration = this.getConf();
		
		boolean issuccess = false;
		
		try {
			Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
			job.setJarByClass(WCMapReduce.class);
			
			FileSystem fs = FileSystem.get(configuration);
			
			if (fs.exists(new Path(args[1]))) {  
				log.info(this.getClass().getSimpleName()+"的输出路径存在,删除它!");
	            fs.delete(new Path(args[1]), true);  
	        }
			
			//set job
			//input
			Path inpath = new Path(args[0]);
			FileInputFormat.addInputPath(job, inpath);
			
			//output
			Path outpath = new Path(args[1]);
			FileOutputFormat.setOutputPath(job, outpath);
			
			//Mapper
			job.setMapperClass(WCMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			//Reducer
			job.setReducerClass(WCReducer.class);
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
		
		//System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.0-cdh5.3.6");
		
		Configuration configuration = new Configuration();
		
		
		
		/*args = new String[]{
				"/user/whl/input/wordcount",
				"/user/whl/output/wordcount"
		};*/
		
		//run job
		try {
			int status = ToolRunner.run(configuration, new WCMapReduce(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
	
	}

}
