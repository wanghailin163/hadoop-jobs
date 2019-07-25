package com.wanghailin.ibeifeng.hadoop.mapreduce.secondarysort;

import java.io.IOException;
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



/**
 * 二次排序
 * @author WangHL
 * c,4                        a,1
 * b,2                        a,4
 * a,4                        a,100
 * b,6         ========>      b,2
 * a,1                        b,6
 * a,100                      c,4
 * c,20                       c,16
 * c,16                       c,20
 */

public class SecondarySort extends Configured implements Tool{
	
	public Logger log = LoggerFactory.getLogger(this.getClass());
	
	public static class SecondaryMapper extends Mapper<LongWritable,Text,PairWritable,IntWritable>{
		
		private PairWritable mapOutputKey = new PairWritable();
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
			
		    mapOutputKey.set(str[0],Integer.valueOf(str[1]));
		    mapOutputValue.set(Integer.valueOf(str[1]));
		    
		    context.write(mapOutputKey,mapOutputValue);
		}
	}
	
	/*public static class SecondaryCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
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
	}*/
	
	public static class SecondaryReducer extends Reducer<PairWritable,IntWritable,Text,IntWritable>{
		private Text outputkey = new Text();
		
		@Override
		protected void reduce(PairWritable key, Iterable<IntWritable> values,Context context)
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
				outputkey.set(key.getFirst());
				context.write(outputkey, value);
			}
		}
	}
	
	public int run(String[] args){
		
		Configuration configuration = this.getConf();
		
		boolean issuccess = false;
		
		try {
			Job job = Job.getInstance(configuration, "secondsort");
			job.setJarByClass(SecondarySort.class);
			
			//set job
			//input
			Path inpath = new Path(args[0]);
			FileInputFormat.addInputPath(job, inpath);
			
			//output
			Path outpath = new Path(args[1]);
			//MR输出路径存在就删除
			FileSystem fs = FileSystem.get(configuration);
			if (fs.exists(outpath)) {  
				log.info("secondsort的MR输出路径存在,删除它!");
	            fs.delete(outpath, true);  
	        }
			FileOutputFormat.setOutputPath(job, outpath);
			
			//Mapper
			job.setMapperClass(SecondaryMapper.class);
			job.setMapOutputKeyClass(PairWritable.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			//  =============shuffle============
			job.setPartitionerClass(FirstPart.class);
			//job.setSortComparatorClass(cls);
			job.setGroupingComparatorClass(FirstGroup.class);
			//job.setCombinerClass(WordSortCombiner.class);
			
			//Reducer
			job.setReducerClass(SecondaryReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			//job.setNumReduceTasks(2);
			
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
				"/user/whl/input/secondarysort/secondarysoryinput",
				"/user/whl/output/secondarysortoutput"
		};*/
		
		//run job
		try {
			int status = ToolRunner.run(configuration, new SecondarySort(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
	
	}

}
