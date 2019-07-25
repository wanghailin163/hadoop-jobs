package com.wanghailin.ibeifeng.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MR实现从Hbase表的数据迁移到另外一张表 
 * eg:user表的 info:name和info:age 数据迁移到 basic表过程
 */
public class User2BasicMapReduce extends Configured implements Tool{

	//Mapper Class
	public static class ReadUserMapper extends TableMapper<Text,Put>{
		
		private Text mapOutputkey = new Text();
		
		/**
		 * key:rowkey
		 * value:一整条数据
		 */
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Put>.Context context)
				throws IOException, InterruptedException {
			// get rowkey
			String rowkey = Bytes.toString(key.get());
			// set
			mapOutputkey.set(rowkey);
			
			//--------------------------------------
			Put put = new Put(key.get());
			
			//iterator
			for(Cell cell:value.rawCells()){	
				//add family : info
				if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
					// add column:name
					if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
						put.add(cell);
					}
					// add column:age
					if("age".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
						put.add(cell);
					}
				}
			}
			
			//context write
			context.write(mapOutputkey, put);
			
		}
		
	}
	
	//Reducer Class
	public static class WriteBasicReducer extends TableReducer<Text,Put,ImmutableBytesWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<Put> values,
				Reducer<Text, Put, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			for(Put put:values){
				context.write(null, put);
			}
			
		}
	}
	
	//Driver
	public int run(String[] arg0) throws Exception {
		
		//create job
		Job job = Job.getInstance(this.getConf(),this.getClass().getSimpleName());
		//set run job class
		job.setJarByClass(this.getClass());
		//set job
		Scan scan = new Scan();
		scan.setCaching(500);//每次获取的条目数
		scan.setCacheBlocks(false);//不设置缓存
		
		//set input and set mapper
		TableMapReduceUtil.initTableMapperJob(
				"user",//input table
				scan,  
				ReadUserMapper.class,//mapper output class
				Text.class,//map output key
				Put.class,//mapper output value
				job 
				);
		
		//set output and set reducer
		TableMapReduceUtil.initTableReducerJob(
				"basic",//output table
				WriteBasicReducer.class,//reduce class
				job
				);
		
		job.setNumReduceTasks(1);//设置reduce数目默认1个
		
		boolean isSuccess = job.waitForCompletion(true);//提交job
		
		return isSuccess ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		//get Configuration
		Configuration configuration = HBaseConfiguration.create();
		
		//submit job
		int status  = ToolRunner.run(configuration, new User2BasicMapReduce(),args);
		
		//exit program
		System.exit(status);
	}
	

}
