package com.wanghailin.myself.hadoop.mapreduce.mapjoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class JoinReducer extends Reducer<CustOrderMapOutKey, Text, CustOrderMapOutKey, Text> {
	@Override
    protected void reduce(CustOrderMapOutKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // 什么事都不用做，直接输出
        for (Text value : values) {
            context.write(key, value);
        }
    }

}
