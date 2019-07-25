package com.ibeifeng.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Udf extends UDF {

	public Text evaluate(Text str){
        return this.evaluate(str, new IntWritable(0));
    }

    public Text evaluate(Text str ,IntWritable flag){

        if(str != null){
            if(flag.get() ==0){
                return new Text(str.toString().toLowerCase());
            }else if(flag.get() ==1){
                return new Text(str.toString().toUpperCase());
            }else
                return null;
        }else
            return null;
    }

    public static void main(String[] args){
        System.out.println(new Udf().evaluate(new Text("hadoop"),new IntWritable(1)));
    }

}
