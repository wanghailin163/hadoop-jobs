package com.ibeifeng.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsApp {
	
	public static Configuration configuration = null;

	//获取配置文件系统
	public static FileSystem getFileSystem() throws Exception {

		// 读取配置信息  default.xml site.xml
	     configuration = new Configuration();
		//configuration.set("fs.defaultFS", "hdfs://master:8020");
		//设置HadoopHome 不然会报错 但是不影响后面结果
		System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.0-cdh5.3.6");
		// 获取文件系统(HDFS)
		FileSystem fileSystem = FileSystem.get(configuration);
		return fileSystem;
	}

	//读取HDFS上的文件  -test
	public static void read(String fileName) throws Exception {

		FileSystem fileSystem = getFileSystem();

		// 获取文件路径
		Path readPath = new Path(fileName);

		// 打开一个输入流
		FSDataInputStream inStream = fileSystem.open(readPath);

		try {
			//数据输出到控制台
			IOUtils.copyBytes(inStream, System.out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
		}
		
	}


	//写文件 -put
	public static void write(String localPath,String hdfsPath) throws Exception {
		FileSystem fileSystem = getFileSystem();

		// 写文件的路径
		Path writePath = new Path(hdfsPath);

		// 获取本地文件内容封装成流
		FileInputStream inStream = new FileInputStream(new File(localPath));

		// 往HDFS上写文件
		FSDataOutputStream outStream = fileSystem.create(writePath);
		try {
			IOUtils.copyBytes(inStream, outStream, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
			IOUtils.closeStream(outStream);
		}
		
	}
	
	
	
	public static void main(String[] args) throws Exception {

		// 读文件
		//String fileName = "/user/whl/datas/emp.txt";
		//read(fileName);

		//写文件
		String localPath = "E:\\oracle.txt";
		String hdfsPath = "/user/whl/tohdfs/oracle.txt";
		write(localPath,hdfsPath);
	
	}
	
	

}
