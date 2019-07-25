package com.wanghailin.myself.hadoop.mapreduce.empdept;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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
 * @author WangHaiLin
 * 求每个部门最早进入公司的员工姓名
 */
@SuppressWarnings("deprecation")
public class Q3DeptEarliestEmp extends Configured implements Tool{
	
	public static Logger log = LoggerFactory.getLogger(Q3DeptEarliestEmp.class);
	
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
			
			//kv[1] 员工姓名      kv[4] 进公司的时间       kv[5] 工资        kv[7] 部门
			kv  = value.toString().split("\t");
			
			// map join: 在map阶段过滤掉不需要的数据，输出key为部门名称和value为员工工资
			// 输出key为部门名称和value为员工姓名+","+员工进入公司日期
			if (deptMap.containsKey(kv[7])) {
				if (null != kv[4] && !"".equals(kv[4].toString())) {
					context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[1].trim()+ "," + kv[4].trim()));
				}
				
			}
		}
	}
	
	public static class ReduceClass extends Reducer<Text,Text,Text,Text>{
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}
		
		Text mapOutputValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			// 员工姓名和进入公司日期
			String empName = null;
			String empEnterDate = null;
			
			// 设置日期转换格式和最早进入公司的员工、日期
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			
			Date earliestDate = new Date();
			String earliestEmp = null;
			
			// 遍历该部门下所有员工，得到最早进入公司的员工信息
			for (Text val : values) {
				empName = val.toString().split(",")[0];
				empEnterDate = val.toString().split(",")[1];
				//log.info(empName+empEnterDate);
				System.out.println(empName+empEnterDate);
				try {
					System.out.println(df.parse(empEnterDate));
					if (df.parse(empEnterDate).compareTo(earliestDate) < 0) {
						earliestDate = df.parse(empEnterDate);
						earliestEmp = empName;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
			mapOutputValue.set("The earliest emp of dept:" + earliestEmp + ", Enter date:" + new SimpleDateFormat("yyyy-MM-dd").format(earliestDate));
			context.write(key, mapOutputValue);
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {

        Configuration configuration = this.getConf();
		
		boolean issuccess = false;
		
		try {
			Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
			job.setJarByClass(Q3DeptEarliestEmp.class);

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
			int status = ToolRunner.run(configuration, new Q3DeptEarliestEmp(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

}
