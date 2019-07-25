package com.myself.hadoop.mapreduce.aggregation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AccountJob extends Configured implements Tool{
	
	public Logger log = LoggerFactory.getLogger(this.getClass());

	@Override
	public int run(String[] arg) throws Exception {
		
		Configuration configuration = this.getConf();
		boolean issuccess = false;
		
		try {
			
			Job job = Job.getInstance(configuration, "ComStar_AccountCount-1");
			job.setJarByClass(AccountJob.class);
			
			Path inpath = new Path(arg[0]);
			FileInputFormat.addInputPath(job, inpath);
			
			Path outpath = new Path(arg[1]);
			//MR输出路径存在就删除
			FileSystem fs = FileSystem.get(configuration);
			if (fs.exists(outpath)) {  
				log.info("ComStar_AccountCount-1的MR输出路径存在,删除它!");
	            fs.delete(outpath, true);  
	        }
			FileOutputFormat.setOutputPath(job, outpath);
			
			job.setMapperClass(AccountMapper1.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			log.info("进入自定义分区!");
			job.setPartitionerClass(AccountPart.class);
			log.info("退出自定义分区!");
			
			job.setReducerClass(AccountReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setNumReduceTasks(9);
			
			if(job.waitForCompletion(true)){
				
				job = Job.getInstance(configuration,"ComStar_AccountCount-2");  
				job.setJarByClass(AccountJob.class);
				
				FileInputFormat.addInputPath(job, new Path(arg[1]));
				//MR输出路径存在就删除
				fs = FileSystem.get(configuration);
				if (fs.exists(new Path(arg[2]))) {  
					log.info("ComStar_AccountCount-2的MR输出路径存在,删除它!");
		            fs.delete(new Path(arg[2]), true);  
		        }
				FileOutputFormat.setOutputPath(job, new Path(arg[2]));
				//第一次的输出是第二次的输入，首次输出的key - value
				job.setInputFormatClass(KeyValueTextInputFormat.class);
				
				job.setMapperClass(AccountMapper2.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);
				
				job.setReducerClass(AccountReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				
				job.setNumReduceTasks(1);
					
				issuccess = job.waitForCompletion(true); 
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		 return issuccess ? 0 : 1;
	}
	
	public static void main(String[] args) {
		
		Configuration configuration = new Configuration();
		
		try {
			int status = ToolRunner.run(configuration, new AccountJob(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
