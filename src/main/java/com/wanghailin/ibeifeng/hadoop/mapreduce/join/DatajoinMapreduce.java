package com.wanghailin.ibeifeng.hadoop.mapreduce.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * mapreduce的join操作
 * @author WangHL
 * customer表                                                                                                                                       order表
 * 用户id        用户名                                     电话联系方式                                                     用户id          商品id           价格                                购买时间
 *  1	 Stephanie Leung	    555-555-5555              3              A              12.95         2008/6/2
 *	2	 Edward Kim	            123-456-7890              1              B              88.25         2008/5/20
 *	3	 Jose Madriz	        281-330-8004              2              C              32            2007/11/30
 *	4	 David Stork	        408-555-0000              3              D              25.02         2009/1/22
 *
 */
public class DatajoinMapreduce extends Configured implements Tool{
	
	public Logger log = LoggerFactory.getLogger(this.getClass());
	
	public static class DatajoinMapper extends Mapper<LongWritable,Text,LongWritable,DatajoinWritable>{
		private LongWritable mapOutputKey = new LongWritable();
		private DatajoinWritable mapOutputValue = new DatajoinWritable();

		
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
			String[] str = lineValue.split(",");
			
			if((str.length!=3)&&(str.length!=4)){
				return;
			}
			
			//get id
			Long cid = Long.valueOf(str[0]);
			
			//get name
			String name = str[1];
			
			//customer
		    if(str.length==3){
		    	String phone = str[2];
		    	
		    	//set
		    	mapOutputKey.set(cid);
		    	mapOutputValue.set("customer", name+","+phone);
		    }
		    
		    //order
		    if(str.length==4){
		    	String price = str[2];
		    	String date = str[3];
		    	
		    	//set
		    	mapOutputKey.set(cid);
		    	mapOutputValue.set("order", name+","+price+","+date);
		    }
		    
		    context.write(mapOutputKey,mapOutputValue);
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
	public static class DatajoinReducer extends Reducer<LongWritable,DatajoinWritable,NullWritable,Text>{
		private Text outputvalue = new Text();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
		}
		
		@Override//Iterable<DatajoinWritable> values 相同cid的数据进行聚合
		protected void reduce(LongWritable key, Iterable<DatajoinWritable> values,Context context)
				throws IOException, InterruptedException {
			String customerInfo = null;
			List<String> orders = new ArrayList<String>();	
			for(DatajoinWritable value : values){
				if("customer".equals(value.getTag())){
					customerInfo = value.getData();
				}else if("order".equals(value.getTag())){
					orders.add(value.getData());
				}
			}
			for(String s : orders){
				outputvalue.set(key.get()+","+customerInfo+","+s);
				context.write(NullWritable.get(), outputvalue);
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
		}
	}
	
	public int run(String[] args){
		
		Configuration configuration = this.getConf();
		
		boolean issuccess = false;
		
		try {
			Job job = Job.getInstance(configuration, "datajoin");
			job.setJarByClass(DatajoinMapreduce.class);
			
			//set job
			//input
			Path inpath = new Path(args[0]);
			FileInputFormat.addInputPath(job, inpath);
			
			//output
			Path outpath = new Path(args[1]);
			//MR输出路径存在就删除
			FileSystem fs = FileSystem.get(configuration);
			if (fs.exists(outpath)) {  
				log.info("datajoin的MR输出路径存在,删除它!");
	            fs.delete(outpath, true);  
	        }
			FileOutputFormat.setOutputPath(job, outpath);
			
			//Mapper
			job.setMapperClass(DatajoinMapper.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(DatajoinWritable.class);
			
			//  =============shuffle============
			//job.setPartitionerClass(cls);
			//job.setSortComparatorClass(cls);
			//job.setGroupingComparatorClass(cls);
			//job.setCombinerClass(WordSortCombiner.class);
			
			//Reducer
			job.setReducerClass(DatajoinReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			//submit job -> yarn
			issuccess = job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return issuccess ? 0 : 1;
	}

	public static void main(String[] args) {
		
		Configuration configuration = new Configuration();
		
		//run job
		try {
			int status = ToolRunner.run(configuration, new DatajoinMapreduce(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
	
	}

}
