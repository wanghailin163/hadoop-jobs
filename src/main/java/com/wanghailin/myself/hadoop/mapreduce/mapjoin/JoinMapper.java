package com.wanghailin.myself.hadoop.mapreduce.mapjoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class JoinMapper extends Mapper<LongWritable, Text, CustOrderMapOutKey, Text> {

    private final CustOrderMapOutKey outputKey = new CustOrderMapOutKey();
    private final Text outputValue = new Text();
    
    // TODO: 改用参数传入
    public static final String CUSTOMER_CACHE_URL = "hdfs://master:8020/user/whl/mapreduce/cache/customer.txt";
    /**
     * 在内存中customer数据
     */
    private static final Map<Integer, CustomerBean> CUSTOMER_MAP = new HashMap<Integer, CustomerBean>();
    
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create(CUSTOMER_CACHE_URL), context.getConfiguration());
        FSDataInputStream fdis = fs.open(new Path(CUSTOMER_CACHE_URL));
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(fdis));
        String line = null;
        String[] cols = null;
        
        // 格式：客户编号  姓名  地址  电话
        while ((line = reader.readLine()) != null) {
            cols = line.split("\t");
            if (cols.length < 4) {              // 数据格式不匹配，忽略
                continue;
            }
            
            CustomerBean bean = new CustomerBean(Integer.parseInt(cols[0]), cols[1], cols[2], cols[3]);
            CUSTOMER_MAP.put(bean.getCustId(), bean);
        }
    }
    
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        // 格式: 订单编号 客户编号    订单金额
        String[] cols = value.toString().split("\t");           
        if (cols.length < 3) {
            return;
        }
        
        int custId = Integer.parseInt(cols[1]);     // 取出客户编号
        CustomerBean customerBean = CUSTOMER_MAP.get(custId);
        
        if (customerBean == null) {         // 没有对应的customer信息可以连接
            return;
        }
        
        StringBuffer sb = new StringBuffer();
        sb.append(cols[2])
            .append("\t")
            .append(customerBean.getName())
            .append("\t")
            .append(customerBean.getAddress())
            .append("\t")
            .append(customerBean.getPhone());
        
        outputValue.set(sb.toString());
        outputKey.set(custId, Integer.parseInt(cols[0]));
        
        context.write(outputKey, outputValue);
    }
    
    


}
