package com.wanghailin.myself.hadoop.mapreduce.empdept;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 求各个部门的平均工资
 * @author WangHaiLin
 */

@SuppressWarnings("deprecation")
public class Q2DeptNumberAveSalary extends Configured implements Tool{
	
	public static Logger log = LoggerFactory.getLogger(Q2DeptNumberAveSalary.class);
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>{
		
		// 用于缓存 dept文件中的数据
		private Map<String, String> deptMap = new HashMap<String, String>();
		private String[] kv;
			
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			BufferedReader in = null;
			
			try {
				// 从当前作业中获取要缓存的文件
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				String deptIdName = null;
				for(Path path : paths){
					//对部门文件字段进行拆分并缓存到deptMap中
					if (path.toString().contains("dept")) {
						in = new BufferedReader(new FileReader(path.toString()));
						while (null != (deptIdName = in.readLine())) {
							// 对部门文件字段进行拆分并缓存到deptMap中
							// 其中Map中key为部门编号，value为所在部门名称
							deptMap.put(deptIdName.split("\t")[0], deptIdName.split("\t")[1]);
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if(in!=null){
						in.close();
					}
				} catch (Exception e2) {
					e2.printStackTrace();
				}
			}

		}
		
		Text mapOutputKey = new Text();
		Text mapOutputValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			//kv[5] 工资   kv[7] 部门
			kv  = value.toString().split("\t");
			
			// map join: 在map阶段过滤掉不需要的数据，输出key为部门名称和value为员工工资
			
			if (deptMap.containsKey(kv[7])) {
				if (null != kv[5] && !"".equals(kv[5].toString())) {
					context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[5].trim()));
				}
				
			}
		}
	}
	
	public static class ReduceClass extends Reducer<Text,Text,Text,DoubleWritable>{
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}
		
		DoubleWritable mapOutputValue = new DoubleWritable();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			
			double sumSalay = 0;
			int sumPerson = 0;
			for(Text value:values){
				sumSalay+=Double.parseDouble(value.toString());
				sumPerson++;
			}
			mapOutputValue.set(sumSalay/sumPerson);
			context.write(key, mapOutputValue);
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {

        Configuration configuration = this.getConf();
		
		boolean issuccess = false;
		
		try {
			Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
			job.setJarByClass(Q2DeptNumberAveSalary.class);

			
			//Mapper
			job.setMapperClass(MapClass.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			//Reducer
			job.setReducerClass(ReduceClass.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			// 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
			String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
			DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
			FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
			FileSystem fs = FileSystem.get(configuration);
			if (fs.exists(new Path(otherArgs[2]))) {  
				log.info(this.getClass().getSimpleName()+"的输出路径存在,删除它!");
	            fs.delete(new Path(otherArgs[2]), true);  
	        }
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
			
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
			int status = ToolRunner.run(configuration, new Q2DeptNumberAveSalary(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

}
