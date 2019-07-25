package com.myself.hadoop.mapreduce.aggregation;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AccountMapper2 extends Mapper<Text, Text, Text, IntWritable> {

	@Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {  
		String word = key.toString();  
        //int index = word.lastIndexOf("_") ;  
        //word = word.substring(0,index) ; 
        
        int count = Integer.parseInt(value.toString()); 
        context.write(new Text(word) , new IntWritable(count)); 
    } 
}
