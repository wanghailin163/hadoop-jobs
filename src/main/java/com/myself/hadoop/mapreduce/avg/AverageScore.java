package com.myself.hadoop.mapreduce.avg;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 计算学生的平均成绩
 * 学生成绩以每科一个文件输入
 * 文件内容为：姓名 成绩
 */
public class AverageScore extends Configured implements Tool{

	
	public static class AverageMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{
		private Text mapOutputKey = new Text();
		private DoubleWritable mapOutputvalue = new DoubleWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String linestr = value.toString();//获取map中每一行的数据
			String[] str = linestr.split("\\|");

			if(str.length!=2){
				return;
			}
			
			String name = str[0];
			String score = str[1];
			
			mapOutputKey.set(name);
			mapOutputvalue.set(Double.valueOf(score));
			
			context.write(mapOutputKey, mapOutputvalue);
			
		}
	}
	
	
	public static class AverageReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
		private Text mapOutputKey = new Text();
		private DoubleWritable mapOutputvalue = new DoubleWritable();
		
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
		
			int count = 0;
			double sum = 0;
			for(DoubleWritable l :values){
				sum+=l.get();
				count++;
				
			}
			mapOutputKey.set(key);
			mapOutputvalue.set(sum/count);
			context.write(mapOutputKey, mapOutputvalue);
		
		}
	}
	
	
	public int run(String[] args) throws Exception {

		Configuration configuration = this.getConf();
		boolean issuccess = false;
		try {
			
			Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
			job.setJarByClass(AverageScore.class);
			
			Path inpath = new Path(args[0]);
			FileInputFormat.addInputPath(job, inpath);
			
			Path outpath = new Path(args[1]);
			FileOutputFormat.setOutputPath(job, outpath);
			
			job.setMapperClass(AverageMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			
			job.setReducerClass(AverageReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			issuccess = job.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return issuccess ? 0 : 1;
	}
	
	
	public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.0-cdh5.3.6");
		
		Configuration configuration = new Configuration();
		
		args = new String[]{
				"/user/whl/input/mapreduce/*",
				"/user/whl/avgscore"
		};
		
		try {
			int status = ToolRunner.run(configuration, new AverageScore(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	

}
