package com.ibeifeng.hive.udf;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


/**
 * 获取当前日期与当年最后一天/当季度最后一天/当月最后一天/当周最后一天差多少天
 * @author WangHaiLin
 */
public class TimeDvalue2 extends UDF {

	/**
	 * 日期装换
	 * @param datetime  实际日期格式       yyyyMMdd
	 * @param timetype  时间类型               Y/Q/M/W
	 * @return
	 */
	public LongWritable evaluate(Text nowDate,Text timetype){
		
		long nd = 1000 * 24 * 60 * 60;//天
	    
	    LongWritable returnday = new LongWritable();
	    
	    Calendar cal = Calendar.getInstance();
	  
		try {
			Date nowDateTime=new SimpleDateFormat("yyyyMMdd").parse(nowDate.toString());
			if("Y".equals(timetype.toString())){			
				cal.setTime(nowDateTime);
				int last = cal.getActualMaximum(Calendar.DAY_OF_YEAR); 
				cal.set(Calendar.DAY_OF_YEAR, last);  
				Date endDateTime=cal.getTime();
				
				long diff = endDateTime.getTime() - nowDateTime.getTime() ;
				
				returnday.set(diff / nd);
				
				return returnday;
			}else if("Q".equals(timetype.toString())){
				/*cal.setTime(nowDateTime); 
			    int month = getQuarterInMonth(cal.get(Calendar.MONTH), false);  
			    cal.set(Calendar.MONTH, month);  
			    cal.set(Calendar.DAY_OF_MONTH, last);
			    
			    Date endDateTime=cal.getTime();
				
				long diff = nowDateTime.getTime() -  endDateTime.getTime() ;
				
				returnday.set(diff / nd);
				
				return returnday;*/
			}else if("M".equals(timetype.toString())){
				cal.setTime(nowDateTime);
				int last = cal.getActualMaximum(Calendar.DAY_OF_MONTH); 
				cal.set(Calendar.DAY_OF_MONTH, last);  
				Date endDateTime=cal.getTime();
				
				long diff = endDateTime.getTime() - nowDateTime.getTime();
				
				returnday.set(diff / nd);
				
				return returnday;
			}else if("W".equals(timetype.toString())){
				cal.setTime(nowDateTime);
				int last = cal.getActualMaximum(Calendar.DAY_OF_WEEK); 
				cal.set(Calendar.DAY_OF_WEEK, last);  
				Date endDateTime=cal.getTime();
				
				long diff = endDateTime.getTime() - nowDateTime.getTime();
				
				returnday.set((diff / nd)+1);
				
				return returnday;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 
	 * @param month
	 * @param isQuarterStart true:季度初   flase:季度末   
	 * @return
	 */
	private static int getQuarterInMonth(int month, boolean isQuarterStart) {  
        int months[] = { 1, 4, 7, 10 };  
        if (!isQuarterStart) {  
            months = new int[] { 3, 6, 9, 12 };  
        }  
        if (month >= 2 && month <= 4)  
            return months[0];  
        else if (month >= 5 && month <= 7)  
            return months[1];  
        else if (month >= 8 && month <= 10)  
            return months[2];  
        else  
            return months[3];  
    }

	public static void main(String[] args) {

		TimeDvalue2 td = new TimeDvalue2();
		
		System.out.println(td.evaluate(new Text("20180527"), new Text("Q")));
	}

}
