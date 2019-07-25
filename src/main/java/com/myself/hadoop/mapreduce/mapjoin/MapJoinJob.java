package com.wanghailin.myself.hadoop.mapreduce.mapjoin;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MapJoinJob extends Configured implements Tool{
	
	public Logger log = LoggerFactory.getLogger(this.getClass());
	
	@Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "mapjoin");
        job.setJarByClass(MapJoinJob.class);
        
        // 添加customer cache文件
        job.addCacheFile(URI.create(JoinMapper.CUSTOMER_CACHE_URL));
        
        Path inpath = new Path(args[0]);
        Path outpath = new Path(args[1]);
        
        FileInputFormat.addInputPath(job, inpath);
        FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outpath)) {  
			log.info("MapJoin的MR输出路径存在,删除它!");
            fs.delete(outpath, true);  
        }
        FileOutputFormat.setOutputPath(job, outpath);
        
        // map settings
        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(CustOrderMapOutKey.class);
        job.setMapOutputValueClass(Text.class);
        
        // reduce settings
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(CustOrderMapOutKey.class);
        job.setOutputKeyClass(Text.class);
        
        boolean res = job.waitForCompletion(true);
        
        return res ? 0 : 1;
    }

	public static void main(String[] args) throws Exception{
		
		if (args.length < 2) {
            new IllegalArgumentException("Usage: <inpath> <outpath>");
            return;
        }
        ToolRunner.run(new Configuration(), new MapJoinJob(), args);
        
	}
	
	

}
