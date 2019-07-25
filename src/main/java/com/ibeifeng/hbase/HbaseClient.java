package com.wanghailin.ibeifeng.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.common.IOUtils;

/**
 * CRUD Operation
 * @author WangHaiLin
 *
 */
public class HbaseClient {
	
	   public static HTable getTable (String name) throws Exception{

	        Configuration conf = HBaseConfiguration.create();

	        HTable table = new HTable(conf,name);

	        return table;
	    }

	    /**
	     * get 'tbname' 'rowkey' 'cf:col'
	     * @param table
	     */
	    public static void getData(HTable table) throws Exception{

	        Get get = new Get(Bytes.toBytes("20170225_10003"));
	        //get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));
	        get.addFamily(Bytes.toBytes("info"));
	        //load the get
	        Result rs = table.get(get);
	        //print the data
	        for(Cell cell : rs.rawCells()){
	            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell))
	                            +"->"+
	                    Bytes.toString(CellUtil.cloneQualifier(cell))
	                            +"->"+
	                            Bytes.toString(CellUtil.cloneValue(cell))
	                                    +"->"+
	                            cell.getTimestamp()
	            );
	            System.out.println("-----------------------------------");
	        }

	        IOUtils.closeStream(table);
	    }

	    /**
	     * put 'tbname' 'rowkey' 'cf:col' 'value'
	     * @param table
	     * @throws Exception
	     */
	    public static void putData(HTable table) throws Exception{

	        Put put = new Put(Bytes.toBytes("20180402_10001"));
	        put.add(Bytes.toBytes("info"),
	                Bytes.toBytes("age"),
	                Bytes.toBytes("26"));
	        put.add(Bytes.toBytes("info"),
	                Bytes.toBytes("name"),
	                Bytes.toBytes("wanghailin"));
	        table.put(put);
	        //print
	        getData(table);
	        
	        IOUtils.closeStream(table);
	    }

	    public static void deleteData(HTable table) throws Exception{

	        Delete del = new Delete(Bytes.toBytes("20170222_10003"));
	        del.deleteColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));//删除列
            //del.deleteFamily(Bytes.toBytes("info"));//删除列族
	        //load the del
	        table.delete(del);
	        //print
	        getData(table);
	        IOUtils.closeStream(table);
	    }

	    public static void scanData(HTable table) throws Exception{

	        Scan scan = new Scan();
	        //load the scan
	        ResultScanner rsscan = table.getScanner(scan);
	        for(Result rs : rsscan){
	            System.out.println(Bytes.toString(rs.getRow()));
	            for(Cell cell : rs.rawCells()){
	                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell))
	                                +"->"+
	                                Bytes.toString(CellUtil.cloneQualifier(cell))
	                                +"->"+
	                                Bytes.toString(CellUtil.cloneValue(cell))
	                                +"->"+
	                                cell.getTimestamp()
	                );
	            }
	            System.out.println("-----------------------------------");
	        }
	        
	        IOUtils.closeStream(rsscan);
	        IOUtils.closeStream(table);

	    }

	    /**
	     * scan 'tbname' ,{STARTROW => 'row1','STOPROW => 'row2''}
	     * @param table
	     * @throws Exception
	     */
	    public static void rangeData(HTable table) throws Exception{

	        Scan scan = new Scan();
	        //Scan scan2 = new Scan(Bytes.toBytes("20170225_10001"),Bytes.toBytes("20170225_10003"));

	        //conf the scan
	        //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
	        scan.setStartRow(Bytes.toBytes("20170225_10001"));
	        scan.setStopRow(Bytes.toBytes("20170225_10003"));
	        
	        //PrefixFilter//前缀过滤器
	        //PageFilter//分页过滤器
	        //scan.setFilter(filter)//过滤器
	        
            //scan.setCacheBlocks(cacheBlocks);//缓存到blockcache
	        //scan.setCaching(caching);//每一次拿这条数据拿多少列
	        
	        //load the scan
	        ResultScanner rsscan = table.getScanner(scan);
	        
	        for(Result rs : rsscan){
	            System.out.println(Bytes.toString(rs.getRow()));
	            for(Cell cell : rs.rawCells()){
	                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell))//列族
	                                +"->"+
	                                Bytes.toString(CellUtil.cloneQualifier(cell))//列
	                                +"->"+
	                                Bytes.toString(CellUtil.cloneValue(cell))//值
	                                +"->"+
	                                cell.getTimestamp()//时间戳
	                );
	            }
	            System.out.println("-----------------------------------");
	        }
	        
	        IOUtils.closeStream(rsscan);
	        IOUtils.closeStream(table);
	    }
	    
	    public static void addcolumn(HTable table) throws Exception{
	    	
	    	Get get = new Get(Bytes.toBytes("20170225_10003"));
	    	
	    	//add column
	    	get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"));
	    	get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("sex"));
	    }

	    public static void main(String[] args) throws Exception{

	        HTable table = getTable("nstest:tb1");
	        //getData(table);
	        //putData(table);
	        //deleteData(table);
	        //scanData(table);
		   //关闭rangeData方法
	        //rangeData(table);
	        addcolumn(table);
	    }
}
