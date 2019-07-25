package com.myself.hadoop.mapreduce.aggregation;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 统计interface_account_main中，每一种资产类型一共有多少笔数据
 * @author WangHaiLin
 * 20180324
 */
public class AccountMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private Text mapOutputKey = new Text();
	private IntWritable mapOutputvalue = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String linestr = value.toString();//获取map中每一行的数据
		String[] str = linestr.split("\\|");
		if(str.length!=18){
			return;
		}
		if(str[9]==null||"".equals(str[9])){
			return;
		}
		//对key重新设计，增加随机数后缀
		//mapOutputKey.set(str[9]+"_"+new Random().nextInt(100));
		mapOutputKey.set(str[9]);
		context.write(mapOutputKey, mapOutputvalue);
	}
	
}
